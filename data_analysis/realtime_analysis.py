import os

import logging

# required for pandas to read csv from aws
from s3path import S3Path
import pandas as pd
import pendulum
from tqdm import tqdm

from utils import s3_csv_reader


BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
BASE_PATH = S3Path(f"/{BUCKET_PUBLIC}")

SCHEDULE_RT_PATH = BASE_PATH / "schedule_rt_comparisons" / "route_level"
SCHEDULE_SUMMARY_PATH = BASE_PATH / "schedule_summaries" / "route_level"



class RealtimeProvider:
    def __init__(self, feed, agg_info):
        self.feed = feed
        self.agg_info = agg_info

    @staticmethod
    # TODO: dedupe
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

    # TODO: dedupe
    def sum_by_frequency(self,
            df: pd.DataFrame,
            ) -> pd.DataFrame:
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
            df.set_index(self.agg_info.byvars)
            .groupby(
                [pd.Grouper(level='date', freq=self.agg_info.freq),
                 pd.Grouper(level='route_id')])[self.agg_info.aggvar]
            .sum().reset_index()
        )
        #print(f'>> sum_by_frequency in {df} >> sum_by_frequency out {out}')
        return out


    def rt_summarize(self, rt_df: pd.DataFrame) -> pd.DataFrame:
        rt_df = rt_df.copy()
        logging.info('rt df')
        rt_freq_by_rte = self.sum_by_frequency(rt_df)
        return rt_freq_by_rte

    def provide(self):
        feed = self.feed.schedule_feed_info
        logging.info(f'Process feed {feed}')
        start_date = feed["feed_start_date"]
        end_date = feed["feed_end_date"]
        date_range = [
            d
            for d in pendulum.period(
                pendulum.from_format(start_date, "YYYY-MM-DD"),
                pendulum.from_format(end_date, "YYYY-MM-DD"),
            ).range("days")
        ]
        #self.pbar.set_description(
        #    f"Loading schedule version {feed['schedule_version']}"
        #)

        rt_raw = pd.DataFrame()
        date_pbar = tqdm(date_range)
        for day in date_pbar:
            date_str = day.to_date_string()
            date_pbar.set_description(
                f" Processing {date_str} at "
                f"{pendulum.now().to_datetime_string()}"
            )

            # realtime bus position data
            daily_data = s3_csv_reader.read_csv(BASE_PATH / f"bus_full_day_data_v2/{date_str}.csv")
            daily_data = self.make_daily_summary(daily_data)

            rt_raw = pd.concat([rt_raw, daily_data])
        if rt_raw.empty:
            return pd.DataFrame(), pd.DataFrame()

        ##print(f'>> combined rt {rt_raw}')

        # basic reformatting
        rt = rt_raw.copy()
        rt["date"] = pd.to_datetime(rt.data_date, format="%Y-%m-%d")
        rt["route_id"] = rt["rt"]

        ##print(f'>> processed rt {rt}')

        # def rt_summarize(rt_df: pd.DataFrame, agg_info: AggInfo) -> pd.DataFrame:
        rt_freq_by_rte = self.rt_summarize(rt)
        return rt_freq_by_rte