import os

from typing import List, Tuple
import logging

from s3path import S3Path
import pandas as pd
import pendulum
from tqdm import tqdm
from dotenv import load_dotenv

import data_analysis.static_gtfs_analysis as static_gtfs_analysis
from data_analysis.file_manager import FileManager
from scrape_data.scrape_schedule_versions import create_schedule_list, ScheduleFeedInfo
from data_analysis.realtime_analysis import RealtimeProvider
from data_analysis.common import AggInfo, sum_by_frequency
#from utils import s3_csv_reader

load_dotenv()

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

BASE_PATH = S3Path(f"/{BUCKET_PUBLIC}")

SCHEDULE_RT_PATH = BASE_PATH / "schedule_rt_comparisons" / "route_level"
SCHEDULE_SUMMARY_PATH = BASE_PATH / "schedule_summaries" / "route_level"


def sched_summarize(sched_df: pd.DataFrame, agg_info: AggInfo) -> pd.DataFrame:
    sched_df = sched_df.copy()
    # maybe this doesn't do anything

    logging.info('sched df')
    sched_freq_by_rte = sum_by_frequency(
        sched_df,
        agg_info=agg_info
    )
    return sched_freq_by_rte


# now the main combiner of parallel dfs of summarized rt and summarized schedule data
def sum_trips_by_rt_by_freq(
    rt_freq_by_rte: pd.DataFrame,
    sched_freq_by_rte: pd.DataFrame,
    holidays: List[str]) -> pd.DataFrame:
    """Calculate ratio of trips to scheduled trips per route
       per specified frequency.

    Args:
        rt_df (pd.DataFrame): A DataFrame of daily route data
        sched_df (pd.DataFrame): A DataFrame of daily scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        holidays (List[str], optional): List of holidays in analyzed period in YYYY-MM-DD format.
            Defaults to ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"].

    Returns:
        pd.DataFrame: DataFrame a row per day per route with the number of scheduled and observed trips. 
        pd.DataFrame: DataFrame with the total number of trips per route
            by specified frequency and the ratio of actual trips to
            scheduled trips.
    """

    compare_freq_by_rte = rt_freq_by_rte.merge(
        sched_freq_by_rte,
        how="inner",
        on=["date", "route_id"],
        suffixes=["_rt", "_sched"],
    )

    # compare by day of week
    compare_freq_by_rte["dayofweek"] = (
        compare_freq_by_rte["date"]
        .dt.dayofweek
    )
    compare_freq_by_rte["day_type"] = (
        compare_freq_by_rte["dayofweek"].map(
            {0: "wk", 1: "wk", 2: "wk", 3: "wk",
                4: "wk", 5: "sat", 6: "sun"})
    )
    compare_freq_by_rte.loc[
        compare_freq_by_rte.date.isin(
            holidays), "day_type"
    ] = "hol"


    # compare_freq_by_rte is important, day can be derived
    return compare_freq_by_rte


def freq_to_day(compare_freq_by_rte: pd.DataFrame) -> pd.DataFrame:
    compare_by_day_type = (
        compare_freq_by_rte.groupby(["route_id", "day_type"])[
            ["trip_count_rt", "trip_count_sched"]
        ]
        .sum()
        .reset_index()
    )

    compare_by_day_type["ratio"] = (
        compare_by_day_type["trip_count_rt"]
        / compare_by_day_type["trip_count_sched"]
    )
    return compare_by_day_type


class Combiner:
    # Read in pre-computed files of RT and scheduled data and compare!
    """Class to generate a combined DataFrame with the realtime route comparisons

    Args:
        schedule_feeds (List[ScheduleFeedInfo]): A list of ScheduleFeedInfo instances,
            each representing a schedule feed covering a specific time period.
        schedule_data_list (List[dict]): A list of dictionaries with a
            "schedule_version" key and "data" key with a value corresponding to
            the daily route summary for that version.
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        holidays (List[str], optional): List of holidays in analyzed period in YYYY-MM-DD format.
            Defaults to ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"].
        save (bool, optional): whether to save the csv file to s3 bucket.
    """
    def __init__(self, provider, agg_info, holidays):
        self.schedule_provider = provider
        self.rt_provider = RealtimeProvider(provider, agg_info)
        self.holidays = holidays
        self.agg_info = agg_info
        #self.compare_by_day_type = None
        self.compare_freq_by_rte = None
        # need to decouple s3 save and local save
        self.save = False
        self.fm = FileManager('combined')
        #self.combine()

    def empty(self):
        return self.compare_freq_by_rte is None

    def retrieve(self):
        filename = f'{self.schedule_provider.schedule_feed_info.schedule_version}_combined.json'
        df = self.fm.retrieve_calculated_dataframe(filename, self.combine, [])
        #df = self.combine()
        self.compare_freq_by_rte = df
        return df

    def combine(self):
        feed = self.schedule_provider.schedule_feed_info
        logging.info(f'Process feed {feed}')
        start_date = feed["feed_start_date"]
        end_date = feed["feed_end_date"]
        # self.pbar.set_description(
        #     f"Loading schedule version {feed['schedule_version']}"
        # )

        schedule = self.schedule_provider.get_route_daily_summary()
        if schedule.empty:
            return pd.DataFrame()
        schedule["date"] = pd.to_datetime(schedule.date, format="%Y-%m-%d")

        sched_freq_by_rte = sched_summarize(schedule, agg_info=self.agg_info)
        rt_freq_by_rte = self.rt_provider.provide()

        compare_freq_by_rte = sum_trips_by_rt_by_freq(
            rt_freq_by_rte=rt_freq_by_rte,
            sched_freq_by_rte=sched_freq_by_rte,
            holidays=self.holidays
        )

        compare_freq_by_rte['feed_version'] = feed['schedule_version']

        if self.save:
            compare_by_day_type = freq_to_day(compare_freq_by_rte)
            outpath = (
                (SCHEDULE_RT_PATH /
                 f'schedule_v{feed["schedule_version"]}_'
                 f"realtime_rt_level_comparison_"
                 f'{feed["feed_start_date"]}_to_'
                 f'{feed["feed_end_date"]}.csv').as_uri())
            compare_by_day_type.to_csv(
                outpath,
                index=False,
            )
        logger.info(f" Processing version {feed['schedule_version']}")
        logger.info('compare_freq_by_rte')
        logger.info(compare_freq_by_rte)
        # should be able to derive compare_by_day_type from compare_freq_by_rte
        return compare_freq_by_rte


class Summarizer:
    def __init__(self, freq: str = 'D', save: bool = True, start_date = None, end_date = None):
        """Calculate the summary by route and day across multiple schedule versions

        Args:
            freq (str): Frequency of aggregation. Defaults to Daily.
            save (bool, optional): whether to save DataFrame to s3.
                Defaults to True.
        """
        self.freq = freq
        self.save = save
        #self.schedule_feeds = create_schedule_list(month=5, year=2022)
        self.schedule_feeds = []
        self.start_date = None
        self.end_date = None
        if start_date:
            self.start_date = start_date.date()
        if end_date:
            self.end_date = end_date.date()
        self.schedule_manager = static_gtfs_analysis.ScheduleManager(month=5, year=2022)
        self.schedule_data_list = []
        self.fm = FileManager('schedule_daily_summary')
        self.agg_info = AggInfo(freq=self.freq)
        self.holidays: List[str] = ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"]

    def build_summary(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Create a summary by route and day type

        Args:
            combined_df (pd.DataFrame): A DataFrame with all schedule versions

        Returns:
            pd.DataFrame: A DataFrame summary across
                versioned schedule comparisons.
        """
        combined_df = combined_df.copy(deep=True)
        print(f'build_summary: from {combined_df}')
        summary = (
            combined_df.groupby(["route_id", "day_type"])[
                ["trip_count_rt", "trip_count_sched"]
            ]
            .sum()
            .reset_index()
        )

        summary["ratio"] = summary["trip_count_rt"] / summary["trip_count_sched"]

        if self.save:
            outpath = (
                (SCHEDULE_RT_PATH /
                 f"combined_schedule_realtime_rt_level_comparison_"
                 f"{pendulum.now()}.csv").as_uri()
            )
            summary.to_csv(
                outpath,
                index=False,
            )
        print(f'build_summary: to {summary}')
        return summary

    def main(self, existing=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate the summary by route and day across multiple schedule versions

        Returns:
            pd.DataFrame: A DataFrame of every day in the specified data with
            scheduled and observed count of trips.
            pd.DataFrame: A DataFrame summary across
                versioned schedule comparisons.
        """
        agg_info = AggInfo(freq=self.freq)
        if existing is not None:
            combined_long = existing
        else:
            combined_long = pd.DataFrame()
        #combined_grouped = pd.DataFrame()
        if self.end_date is not None:
            logger.info(f'Filtering to {self.end_date}')

        # fix feed date types
        for feed in tqdm(self.schedule_manager.generate_providers()):
            new_start_date = None
            new_end_date = None
            print(f'feed {feed.start_date().date()} sd {self.start_date}')
            if self.start_date is not None:
                if feed.end_date().date() < self.start_date:
                    logger.info(f'Skipping out-of-range feed {feed.schedule_feed_info}')
                    continue
                if self.start_date > feed.start_date().date():
                    new_start_date = self.start_date
            if self.end_date is not None:
                if feed.start_date().date() > self.end_date:
                    logger.info(f'Skipping out-of-range feed {feed.schedule_feed_info}')
                    continue
                if self.end_date < feed.end_date().date():
                    new_end_date = self.end_date
            if new_start_date:
                print(f'Using alt start date {new_start_date}')
            if new_end_date:
                print(f'Using alt end date {new_end_date}')
            combiner = Combiner(feed, agg_info, self.holidays)
            this_iter = combiner.retrieve()
            if this_iter.empty:
                continue
            if new_start_date:
                this_iter = this_iter[this_iter.date >= new_start_date.strftime('%Y%m%d')]
            if new_end_date:
                this_iter = this_iter[this_iter.date <= new_end_date.strftime('%Y%m%d')]
            combined_long = pd.concat([combined_long, this_iter])
        combined_grouped = freq_to_day(combined_long)
        return combined_long, self.build_summary(combined_grouped)


def main(freq: str = 'D', save: bool = False, start_date = None, end_date = None, existing=None):
    summarizer = Summarizer(freq, save, start_date, end_date)
    return summarizer.main(existing)

if __name__ == "__main__":
    main()
