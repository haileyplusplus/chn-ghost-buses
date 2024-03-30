import os

from dataclasses import dataclass, field
from typing import List, Tuple
import logging
from functools import partial

# required for pandas to read csv from aws
#import s3fs
from s3path import S3Path
import pandas as pd
import pendulum
from tqdm import tqdm
from dotenv import load_dotenv

import data_analysis.static_gtfs_analysis as static_gtfs_analysis
from scrape_data.scrape_schedule_versions import create_schedule_list, ScheduleFeedInfo
from data_analysis.realtime_analysis import RealtimeProvider
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


@dataclass
class AggInfo:
    """A class for storing information about
        aggregation of route and schedule data

    Args:
        freq (str, optional): An offset alias described in the Pandas
            time series docs. Defaults to None.
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        aggvar (str, optional): variable to aggregate by.
            Defaults to trip_count
        byvars (List[str], optional): variables to passed to
            pd.DataFrame.groupby. Defaults to ['date', 'route_id'].
    """
    freq: str = 'D'
    aggvar: str = 'trip_count'
    byvars: List[str] = field(default_factory=lambda: ['date', 'route_id'])


def make_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Make a summary of trips that actually happened. The result will be
        used as base data for further aggregations.

    Args:
        df (pd.DataFrame): A DataFrame read from bus_full_day_data_v2/{date}.

    Returns:
        pd.DataFrame: A summary of full day data by
            date, route, and destination.
    """
    #print(f'>> make_daily_summary in {df}')
    df = df.copy()
    df = (
        df.groupby(["data_date", "rt"])
        .agg({"vid": set, "tatripid": set, "tablockid": set})
        .reset_index()
    )
    df["vh_count"] = df["vid"].apply(len)
    df["trip_count"] = df["tatripid"].apply(len)
    df["block_count"] = df["tablockid"].apply(len)
    #print(f'>> make_daily_summary out {df}')
    return df


def sum_by_frequency(
    df: pd.DataFrame,
        agg_info: AggInfo) -> pd.DataFrame:
    """Calculate total trips per route per frequency

    Args:
        df (pd.DataFrame): A DataFrame of route or scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.

    Returns:
        pd.DataFrame: A DataFrame with the total number of trips per route
            by a specified frequency.
    """
    df = df.copy()
    logging.info(df)
    out = (
        df.set_index(agg_info.byvars)
        .groupby(
            [pd.Grouper(level='date', freq=agg_info.freq),
                pd.Grouper(level='route_id')])[agg_info.aggvar]
        .sum().reset_index()
    )
    ##print(f'>> sum_by_frequency in {df} >> sum_by_frequency out {out}')
    return out

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

    #print(f'>> sum_trips_by_rt_by_freq: in rt_df {rt_freq_by_rte} >> strt in {sched_freq_by_rte}')

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
    #print(f'>> sum_trips_by_rt_by_freq: st out compare_freq_by_rte {compare_freq_by_rte} >> st out compare_by_day_type {compare_by_day_type}')
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
    # def __init__(
    #     self,
    #     schedule_feeds: List[ScheduleFeedInfo],
    #     schedule_data_list: List[dict],
    #     agg_info: AggInfo,
    #
    #         save: bool = True):
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
    # self.pbar = tqdm(schedule_feeds)
    # self.schedule_data_list = schedule_data_list
    # self.agg_info = agg_info
    # self.holidays = holidays
    # self.save = save
    # self.fm = static_gtfs_analysis.FileManager('combined')
    def __init__(self, provider, agg_info, holidays):
        self.schedule_provider = provider
        self.rt_provider = RealtimeProvider(provider, agg_info)
        self.holidays = holidays
        self.agg_info = agg_info
        #self.compare_by_day_type = None
        self.compare_freq_by_rte = None
        # need to decouple s3 save and local save
        self.save = False
        self.fm = static_gtfs_analysis.FileManager('combined')
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

        # schedule_raw = (
        #     next(
        #         data_dict["data"] for data_dict in self.schedule_data_list
        #         if feed["schedule_version"] == data_dict["schedule_version"]
        #     )
        # )

        #print(f'>> combined rt {rt_raw}')

        # basic reformatting
        #schedule = schedule_raw.copy()
        schedule = self.schedule_provider.get_route_daily_summary()
        if schedule.empty:
            return pd.DataFrame()
        # if schedule.empty:
        #     return pd.DataFrame(), pd.DataFrame()
        schedule["date"] = pd.to_datetime(schedule.date, format="%Y-%m-%d")

        #print(f'>> processed rt {rt}')

        # def rt_summarize(rt_df: pd.DataFrame, agg_info: AggInfo) -> pd.DataFrame:
        sched_freq_by_rte = sched_summarize(schedule, agg_info=self.agg_info)
        rt_freq_by_rte = self.rt_provider.provide()

        compare_freq_by_rte = sum_trips_by_rt_by_freq(
            rt_freq_by_rte=rt_freq_by_rte,
            sched_freq_by_rte=sched_freq_by_rte,
            holidays=self.holidays
        )

        #compare_by_day_type['feed_version'] = feed['schedule_version']
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
        # logger.info('compare_by_day_type')
        # logger.info(compare_by_day_type)
        logger.info('compare_freq_by_rte')
        logger.info(compare_freq_by_rte)
        # should be able to derive compare_by_day_type from compare_freq_by_rte
        #self.compare_by_day_type = compare_by_day_type
        #self.compare_freq_by_rte = compare_freq_by_rte
        return compare_freq_by_rte


class Summarizer:
    def __init__(self, freq: str = 'D', save: bool = True):
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
        self.schedule_manager = static_gtfs_analysis.ScheduleManager(month=5, year=2022)
        self.schedule_data_list = []
        #self.pbar = tqdm(self.schedule_feeds)
        self.fm = static_gtfs_analysis.FileManager('schedule_daily_summary')
        self.agg_info = AggInfo(freq=self.freq)
        self.holidays: List[str] = ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"]

        # combined_long = pd.DataFrame()
        # combined_grouped = pd.DataFrame()
        # logging.info('Compare rt')
        # for feed in self.pbar:
        #     logging.info(f'Compare rt feed {feed}')
        #     logging.info(f'fn 1:')
        #     logging.info(f'{feed.schedule_version}_day.json')
        #     # slightly inefficient but easier to cache
        #     pfday = lambda: partial(self.process_one_feed, feed)()[0]
        #     pffreq = lambda: partial(self.process_one_feed, feed)()[1]
        #     compare_by_day_type = self.fm.retrieve_calculated_dataframe(f'{feed.schedule_version}_day.json',
        #                                                                 pfday, [])
        #     compare_freq_by_rte = self.fm.retrieve_calculated_dataframe(f'{feed.schedule_version}_freq.json',
        #                                                                 pffreq, [])
        #     #compare_by_day_type, compare_freq_by_rte = self.process_one_feed(feed)
        #     combined_grouped = pd.concat([combined_grouped, compare_by_day_type])
        #     combined_long = pd.concat([combined_long, compare_freq_by_rte])
        # return combined_long, combined_grouped

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

    # def create_route_daily_summary(self, feed) -> pd.DataFrame:
    #     schedule_version = feed["schedule_version"]
    #     self.pbar.set_description(
    #         f"Generating daily schedule data for "
    #         f"schedule version {schedule_version}"
    #     )
    #     logger.info(
    #         f"\nDownloading zip file for schedule version "
    #         f"{schedule_version}"
    #     )
    #     schedule = static_gtfs_analysis.ScheduleProvider(feed)
    #     return schedule.get_route_daily_summary()

    def main(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate the summary by route and day across multiple schedule versions

        Returns:
            pd.DataFrame: A DataFrame of every day in the specified data with
            scheduled and observed count of trips.
            pd.DataFrame: A DataFrame summary across
                versioned schedule comparisons.
        """
        agg_info = AggInfo(freq=self.freq)
        combined_long = pd.DataFrame()
        #combined_grouped = pd.DataFrame()

        for feed in tqdm(self.schedule_manager.generate_providers()):
            schedule_version = feed.schedule_version()
            #dailygetter = partial(self.create_route_daily_summary, feed)
            # dailygetter = feed.get_route_daily_summary
            # filename = f'{schedule_version}.json'
            # logger.info(f'csrt main attempting to retrieve top-level {filename}')
            # route_daily_summary = self.fm.retrieve_calculated_dataframe(filename, dailygetter, [])
            combiner = Combiner(feed, agg_info, self.holidays)
            #combined_grouped = pd.concat([combined_grouped, combiner.compare_by_day_type])
            combined_long = pd.concat([combined_long, combiner.retrieve()])
            #print(f'>> this combiner day {combiner.compare_by_day_type}')
            print(f'>> this combiner freq {combiner.compare_freq_by_rte}')
            #route_daily_summary = self.create_route_daily_summary(feed)
            #self.schedule_feeds.append(feed.schedule_feed_info)
            #self.schedule_data_list.append(
            #    {"schedule_version": schedule_version,
            #     "data": route_daily_summary}
            #)
        # combiner = Combiner(
        #     self.schedule_feeds,
        #     schedule_data_list=self.schedule_data_list,
        #     agg_info=agg_info,
        #     save=self.save
        # )
        #combined_long, combined_grouped = combiner.combine_real_time_rt_comparison()
        combined_grouped = freq_to_day(combined_long)
        return combined_long, self.build_summary(combined_grouped)


def main(freq: str = 'D', save: bool = False):
    summarizer = Summarizer(freq, save)
    return summarizer.main()

"""
Pending bug to fix

Summarizing trip data
INFO:root:Callling make_trip_summary with 2022-10-21T00:00:00+00:00, 2022-11-02T00:00:00+00:00
INFO:root:Retrieved trip_summary_20221021_to_20221102.json from cache
14it [01:06,  4.73s/it]
Traceback (most recent call last):
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/.venv/lib/python3.11/site-packages/pandas/core/arrays/datetimes.py", line 2224, in objects_to_datetime64ns
    result, tz_parsed = tslib.array_to_datetime(
                        ^^^^^^^^^^^^^^^^^^^^^^^^
  File "pandas/_libs/tslib.pyx", line 381, in pandas._libs.tslib.array_to_datetime
  File "pandas/_libs/tslib.pyx", line 469, in pandas._libs.tslib.array_to_datetime
ValueError: Tz-aware datetime.datetime cannot be converted to datetime64 unless utc=True

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/update_data.py", line 358, in <module>
    main()
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/update_data.py", line 326, in main
    combined_long_df, summary_df = csrt.main(freq="D")
                                   ^^^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/compare_scheduled_and_rt.py", line 432, in main
    return summarizer.main()
           ^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/compare_scheduled_and_rt.py", line 409, in main
    dailygetter = feed.get_route_daily_summary()
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/static_gtfs_analysis.py", line 235, in get_route_daily_summary
    trip_summary = self.make_trip_summary(
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/static_gtfs_analysis.py", line 259, in make_trip_summary
    return self.file_manager.retrieve_calculated_dataframe(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/static_gtfs_analysis.py", line 89, in retrieve_calculated_dataframe
    df = self.fix_dt_column(df, c)
         ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/hailey/forkedcode/stage/chn-ghost-buses/data_analysis/static_gtfs_analysis.py", line 74, in fix_dt_column
    df[c] = df[c].apply(fixer)
            ^^^^^^^^^^^^^^^^^^

"""

if __name__ == "__main__":
    main()
