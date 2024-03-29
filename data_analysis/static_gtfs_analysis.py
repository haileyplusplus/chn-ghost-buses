# Goals:
#
# * Make a way to calculate the scheduled number of current active trips given
#  a date, time, and route.
#     - Take datetime and find what services are active on that date
#     - Find what trips run on those services + route
#     - Find which of those trips are "in progress" per stop_times
# * ~Output most common shape by route~


# imports
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Tuple
from functools import partial

import logging
import calendar
import datetime

import pandas
import pandas as pd
import zipfile
import requests
import pendulum
from io import BytesIO
import shapely
import geopandas

from tqdm import tqdm
from scrape_data.scrape_schedule_versions import create_schedule_list, ScheduleFeedInfo

VERSION_ID = "20220718"
BUCKET = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
DATA_DIR = Path(__file__).parent.parent / "data_output" / "scratch"

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

IGNORE = '20230211'
#IGNORE = 's'

class FileManager:
    def __init__(self, subdir):
        self.cache_dir = DATA_DIR / subdir
        if not self.cache_dir.exists():
            self.cache_dir.mkdir()

    def retrieve(self, filename: str, url: str) -> BytesIO:
        filepath = self.cache_dir / filename
        if filepath.exists():
            logging.info(f'Retrieved cached {url} from {filename}')
            return BytesIO(filepath.open('rb').read())
        bytes_io = BytesIO(requests.get(url).content)
        with filepath.open('wb') as ofh:
            ofh.write(bytes_io.getvalue())
        logging.info(f'Stored cached {url} in {filename}')
        return bytes_io

    @staticmethod
    def fix_dt_column(df, c):
        def fixer(x):
            if type(x) is not int:
                return pd.NaT
            return datetime.datetime.fromtimestamp(x / 1000).astimezone(datetime.UTC)
        df[c] = df[c].apply(fixer)
        return df

    def retrieve_calculated_dataframe(self, filename, func, dt_fields: list[str]) -> pd.DataFrame:
        filepath = self.cache_dir / filename
        if filename.replace('-', '').startswith(IGNORE):
            print(f'Ignoring whether {filename} is in cache')
            return func()
        if filepath.exists():
            logging.info(f'Retrieved {filename} from cache')
            df = pandas.read_json(filepath)
            assert type(df) is pd.DataFrame
            if df.empty:
                return pd.DataFrame()
            for c in dt_fields:
                df = self.fix_dt_column(df, c)
            #print('Retrieved df')
            #print(df)
            return df
        logging.info(f'Writing {filename} to cache')
        df = func()
        df.to_json(filepath)
        #print(f'Storing df')
        #print(df)
        return df


@dataclass
class GTFSFeed:
    """Class for storing GTFSFeed data.
    """
    stops: pd.DataFrame
    stop_times: pd.DataFrame
    routes: pd.DataFrame
    trips: pd.DataFrame
    calendar: pd.DataFrame
    calendar_dates: pd.DataFrame
    shapes: pd.DataFrame

    @classmethod
    def extract_data(cls, gtfs_zipfile: zipfile.ZipFile,
                     version_id: str = None, cta_download: bool = True) -> GTFSFeed:
        """Load each text file in zipfile into a DataFrame

        Args:
            gtfs_zipfile (zipfile.ZipFile): Zipfile downloaded from
                transitfeeds.com or transitchicago.com e.g.
                https://transitfeeds.com/p/chicago-transit-authority/
                165/20220718/download or https://www.transitchicago.com/downloads/sch_data/
            version_id (str, optional): The schedule version in use.
                Defaults to None.

        Returns:
            GTFSFeed: A GTFSFeed object containing multiple DataFrames
                accessible by name.
        """
        if cta_download:
            if version_id is not None:
                raise ValueError("version_id is not used for downloads directly from CTA")
            else:
                logging.info(f"Extracting data from transitchicago.com zipfile")
        
        else:
            if version_id is None:
                version_id = VERSION_ID
            logging.info(f"Extracting data from transitfeeds.com zipfile version {version_id}")

        data_dict = {}
        pbar = tqdm(cls.__annotations__.keys())
        for txt_file in pbar:
            pbar.set_description(f'Loading {txt_file}.txt')
            try:
                with gtfs_zipfile.open(f'{txt_file}.txt') as file:
                    df = pd.read_csv(file, dtype="object")
                    logger.info(f'{txt_file}.txt loaded')

            except KeyError as ke:
                logger.info(f"{gtfs_zipfile} is missing required file")
                logger.info(ke)
                df = None
            data_dict[txt_file] = df
        return cls(**data_dict)


# Basic data transformations
# Ex. creating actual timestamps
def get_hour(s: str) -> int:
    """Return the hour from a string timestamp

    Args:
        s (str): A string of a timestamp e.g. "HH:MM:SS"

    Returns:
        int: the hour of the timestamp
    """
    parts = s.split(":")
    assert len(parts) == 3
    hour = int(parts[0])
    if hour >= 24:
        hour -= 24
    return hour


def format_dates_hours(data: GTFSFeed) -> GTFSFeed:
    """ Convert string dates to actual datetimes in calendar.txt and
        calendar_dates.txt
    Args:
        data (GTFSFeed): GTFS data from CTA

    Returns:
        GTFSFeed: GTFS data with datetimes and arrival and departue hours.
    """

    data.calendar["start_date_dt"] = data.calendar["start_date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )
    data.calendar["end_date_dt"] = data.calendar["end_date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )
    data.calendar_dates["date_dt"] = data.calendar_dates["date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )

    # extract hour from stop_times timestamps
    data.stop_times["arrival_hour"] = data.stop_times.arrival_time.apply(
        lambda x: get_hour(x)
    )
    data.stop_times["departure_hour"] = data.stop_times.departure_time.apply(
        lambda x: get_hour(x)
    )

    return data


class ScheduleProvider:
    def __init__(self, schedule_feed_info : ScheduleFeedInfo):
        self.gtfs_feed = None
        self.file_manager = FileManager("schedules")
        self.schedule_feed_info = schedule_feed_info

    # def defer_schedule_extraction(self, *args):
    #     # cta_zipfile, version_id, cta_download
    #     self.deferred = args

    def get_route_daily_summary(self):
        cta_gtfs = self.download_zip()

        logger.info("\nMaybe extracting data")
        # schedule.defer_schedule_extraction(
        #     cta_gtfs,
        #     schedule_version,
        #     False
        # )
        #data = static_gtfs_analysis.format_dates_hours(data)

        logger.info("\nSummarizing trip data")

        feed = self.schedule_feed_info
        trip_summary = self.make_trip_summary(
            pendulum.from_format(feed['feed_start_date'], 'YYYY-MM-DD'),
            pendulum.from_format(feed['feed_end_date'], 'YYYY-MM-DD'))

        route_daily_summary = (
            self
            .summarize_date_rt(trip_summary)
        )
        route_daily_summary['version'] = self.schedule_feed_info.schedule_version
        print(f'RDS: {feed} -> {route_daily_summary}')
        return route_daily_summary

    def make_trip_summary(self,
                          feed_start_date: pendulum.datetime = None,
                          feed_end_date: pendulum.datetime = None) -> pd.DataFrame:
        logging.info(f'Callling make_trip_summary with {feed_start_date}, {feed_end_date}')
        start_str = 'none'
        if feed_start_date:
            start_str = feed_start_date.format('YYYYMMDD')
        end_str = 'none'
        if feed_end_date:
            end_str = feed_end_date.format('YYYYMMDD')
        filename = f'trip_summary_{start_str}_to_{end_str}.json'
        func = partial(self.make_trip_summary_inner, feed_start_date, feed_end_date)
        return self.file_manager.retrieve_calculated_dataframe(
            filename, func, ['raw_date', 'start_date_dt', 'end_date_dt'])

    def make_trip_summary_inner(
        self,
        feed_start_date: pendulum.datetime = None,
        feed_end_date: pendulum.datetime = None) -> pd.DataFrame:
        """Create a summary of trips with one row per date

        Args:
            data (GTFSFeed): GTFS data from CTA
            feed_start_date (datetime): Date from which this feed is valid (inclusive).
                Defaults to None
            feed_end_date (datetime): Date until which this feed is valid (inclusive).
                Defaults to None

        Returns:
            pd.DataFrame: A DataFrame with each trip that occurred per row.
        """
        if self.gtfs_feed is None:
            self.gtfs_feed = GTFSFeed.extract_data(*self.deferred)
            self.gtfs_feed = format_dates_hours(self.gtfs_feed)
        assert self.gtfs_feed is not None
        data = self.gtfs_feed
        logging.info(f'Callling make_trip_summary_inner with {feed_start_date}, {feed_end_date}')
        # construct a datetime index that has every day between calendar start and
        # end
        calendar_date_range = pd.DataFrame(
            pd.date_range(
                min(data.calendar.start_date_dt),
                max(data.calendar.end_date_dt)
            ),
            columns=["raw_date"],
        )

        # cross join calendar index with actual calendar to get all combos of
        # possible dates & services
        calendar_cross = calendar_date_range.merge(data.calendar, how="cross")

        # extract day of week from date index date
        calendar_cross["dayofweek"] = calendar_cross["raw_date"].dt.dayofweek

        # take wide calendar data (one col per day of week) and make it long (one
        # row per day of week)
        actual_service = calendar_cross.melt(
            id_vars=[
                "raw_date",
                "start_date_dt",
                "end_date_dt",
                "start_date",
                "end_date",
                "service_id",
                "dayofweek",
            ],
            var_name="cal_dayofweek",
            value_name="cal_val",
        )

        # map the calendar input strings to day of week integers to align w pandas
        # dayofweek output
        actual_service["cal_daynum"] = (
            actual_service["cal_dayofweek"].str.title().map(
                dict(zip(calendar.day_name, range(7)))
            )
        )
        # now check for rows that "work"
        # i.e., the day of week matches between datetime index & calendar input
        # and the datetime index is between the calendar row's start and end dates
        actual_service = actual_service[
            (actual_service.dayofweek == actual_service.cal_daynum)
            & (actual_service.start_date_dt <= actual_service.raw_date)
            & (actual_service.end_date_dt >= actual_service.raw_date)
        ]

        # now merge in calendar dates to the datetime index to get overrides
        actual_service = actual_service.merge(
            data.calendar_dates,
            how="outer",
            left_on=["raw_date", "service_id"],
            right_on=["date_dt", "service_id"],
        )

        # now add a service happened flag for dates where the schedule
        # indicates that this service occurred
        # i.e.: calendar has a service indicator of 1 and there's no
        # exception type from calendar_dates
        # OR calendar_dates has exception type of 1
        # otherwise no service
        # https://stackoverflow.com/questions/21415661/logical-operators-for-boolean-indexing-in-pandas
        actual_service["service_happened"] = (
            (actual_service["cal_val"] == "1")
            & (actual_service["exception_type"].isnull())
        ) | (actual_service["exception_type"] == "1")

        # now fill in rows where calendar_dates had a date outside the bounds of
        # the datetime index, so raw_date is always populated
        actual_service["raw_date"] = actual_service["raw_date"].fillna(
            actual_service["date_dt"]
        )

        # filter to only rows where service occurred
        service_happened = actual_service[actual_service.service_happened]

        # join trips to only service that occurred
        trips_happened = data.trips.merge(
            service_happened, how="left", on="service_id")

        # get only the trip / hour combos that actually occurred
        trip_stop_hours = data.stop_times.drop_duplicates(
            ["trip_id", "arrival_hour"]
        )
        # now join
        # result has one row per date + row from trips.txt (incl. route) + hour
        trip_summary = trips_happened.merge(
            trip_stop_hours, how="left", on="trip_id")

        # filter to only the rows for the period where this specific feed version was in effect
        if feed_start_date is not None and feed_end_date is not None:
            trip_summary = trip_summary.loc[
                (trip_summary['raw_date'] >= feed_start_date)
                & (trip_summary['raw_date'] <= feed_end_date), :]

        return trip_summary

    @staticmethod
    def download_cta_zip() -> Tuple[zipfile.ZipFile, BytesIO]:
        """Download CTA schedule data from transitchicago.com

        Returns:
            zipfile.ZipFile: A zipfile of the latest GTFS schedule data from transitchicago.com
        """
        logger.info('Downloading CTA data')
        fm = FileManager("downloads")
        zip_bytes_io = fm.retrieve(
            'google_transit.zip',
            "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        )
        cta_gtfs = zipfile.ZipFile(zip_bytes_io)
        logging.info('Download complete')
        return cta_gtfs, zip_bytes_io

    def download_zip(self) -> zipfile.ZipFile:
        """Download a version schedule from transitfeeds.com

        Args:
            version_id (str): The version schedule in the form
                of a date e.g. YYYYMMDD

        Returns:
            zipfile.ZipFile: A zipfile for the CTA version id.
        """
        version_id = self.schedule_feed_info.schedule_version
        logger.info('Downloading CTA data')
        fm = FileManager("downloads")
        cta_gtfs = zipfile.ZipFile(
            fm.retrieve(f'{version_id}.zip',
                        f"https://transitfeeds.com/p/chicago-transit-authority"
                        f"/165/{version_id}/download"
                        )
        )
        logging.info('Download complete')
        return cta_gtfs


    def download_extract_format(self) -> GTFSFeed:
        """Download a zipfile of GTFS data for a given version_id,
            extract data, and format date column.

        Args:
            version_id (str): The version of the GTFS schedule data to download. Defaults to None
                If version_id is None, data will be downloaded from the CTA directly (transitchicag.com)
                instead of transitfeeds.com

        Returns:
            GTFSFeed: A GTFSFeed object with formated dates
        """
        if self.schedule_feed_info is None:
            cta_gtfs, _ = self.download_cta_zip()
            version_id = None
        else:
            cta_gtfs = self.download_zip()
            version_id = self.schedule_feed_info.schedule_version
        data = GTFSFeed.extract_data(cta_gtfs, version_id=version_id)
        data = format_dates_hours(data)
        return data

    @staticmethod
    def group_trips(
        trip_summary: pd.DataFrame,
            groupby_vars: list) -> pd.DataFrame:
        """Generate summary grouped by groupby_vars

        Args:
            trip_summary (pd.DataFrame): A DataFrame of one trip per row i.e.
                the output of the make_trip_summary function.
            groupby_vars (list): Variables to group by.

        Returns:
            pd.DataFrame: A DataFrame with the trip count by groupby_vars e.g.
                route and date.
        """
        if trip_summary.empty:
            return pd.DataFrame()
        trip_summary = trip_summary.copy()
        summary = (
            trip_summary.groupby(by=groupby_vars)
            ["trip_id"]
            .nunique()
            .reset_index()
        )

        summary.rename(
            columns={
                "trip_id": "trip_count",
                "raw_date": "date"},
            inplace=True
        )
        print(f'summary date: {summary.date}, type {type(summary.date)}')
        summary.date = summary.date.dt.date
        return summary

    @staticmethod
    def summarize_date_rt(trip_summary: pd.DataFrame) -> pd.DataFrame:
        """Summarize trips by date and route

        Args:
            trip_summary (pd.DataFrame): a summary of trips with one row per date.
                Output of the make_trip_summary function.

        Returns:
            pd.DataFrame: A DataFrame grouped by date and route
        """
        trip_summary = trip_summary.copy()
        groupby_vars = ["raw_date", "route_id"]

        # group to get trips by date by route
        route_daily_summary = ScheduleProvider.group_trips(
            trip_summary,
            groupby_vars=groupby_vars,
        )

        return route_daily_summary


def make_linestring_of_points(
        sub_df: pd.DataFrame) -> shapely.geometry.LineString:
    """Draw a linestring from a DataFrame of points on a route

    Args:
        sub_df (pd.DataFrame): shapes data from CTA grouped by shape_id.

    Returns:
        shapely.geometry.LineString: A Shape constructed
            from points by shape_id.
    """
    sub_df = sub_df.copy()
    sorted_df = sub_df.sort_values(by="shape_pt_sequence")
    return shapely.geometry.LineString(list(sorted_df["pt"]))


def main() -> geopandas.GeoDataFrame:
    """Download data from CTA, construct shapes from shape data,
    and save to geojson file

    Returns:
        geopandas.GeoDataFrame: DataFrame with route shapes
    """

    schedule_list = create_schedule_list(5, 2022)
    # Get the latest version
    latest = schedule_list[-1]
    provider = ScheduleProvider(None)

    data = provider.download_extract_format()

    # check that there are no dwell periods that cross hour boundary
    cross_hr_bndary = (
        data.stop_times[data.stop_times.arrival_hour !=
                        data.stop_times.departure_hour]
    )
    if not cross_hr_bndary.empty:
        logging.warn(
            f"There are dwell periods that cross "
            f"the hour boundary. See {cross_hr_bndary}")

    # ## Most common shape by route

    # get trip count by route, direction, shape id
    trips_by_rte_direction = (
        data.trips.groupby(["route_id", "shape_id", "direction"])["trip_id"]
        .count()
        .reset_index()
    )

    # keep only most common shape id by route, direction
    # follow: https://stackoverflow.com/a/54041328
    most_common_shapes = trips_by_rte_direction.sort_values(
        "trip_id").drop_duplicates(["route_id", "direction"], keep="last")

    # get additional route attributes
    most_common_shapes = most_common_shapes.merge(
        data.routes, how="left", on="route_id"
    )

    # make shapely points
    # https://www.geeksforgeeks.org/apply-function-to-every-row-in-a-pandas-dataframe/
    data.shapes["pt"] = data.shapes.apply(
        lambda row: shapely.geometry.Point(
            (float(row["shape_pt_lon"]), float(row["shape_pt_lat"]))
        ),
        axis=1,
    )

    data.shapes["shape_pt_sequence"] = pd.to_numeric(
        data.shapes["shape_pt_sequence"])

    # construct sorted list of shapely points
    # custom aggregation function: https://stackoverflow.com/a/10964938
    constructed_shapes = (data.shapes.groupby("shape_id").apply(
        make_linestring_of_points).reset_index())

    # merge in the other route attributes
    final = most_common_shapes.merge(
        constructed_shapes, how="left", on="shape_id")

    # make a "geometry" column for geopandas
    final["geometry"] = final[0]

    # construct the geopandas geodataframe
    final_gdf = geopandas.GeoDataFrame(data=final)

    # drop the column that's a list of shapely points
    final_gdf = final_gdf.drop(0, axis=1)

    # https://gis.stackexchange.com/questions/11910/meaning-of-simplifys-tolerance-parameter
    final_gdf["geometry"] = final_gdf["geometry"].simplify(0.0001)

    save_path = (
        DATA_DIR /
        f"route_shapes_simplified_linestring"
        f"_{pendulum.now().strftime('%Y-%m-%d-%H:%M:%S')}.geojson"
    )
    with open(str(save_path), "w") as f:
        f.write(final_gdf.loc[(final_gdf["route_type"] == "3")].to_json())

    logging.info(f'geojson saved to {save_path}')

    return final_gdf


if __name__ == "__main__":
    main()
