from datetime import datetime
import os
from pathlib import Path
from typing import Union, List

import matplotlib
matplotlib.use('QtAgg')

import logging
import folium
from branca.colormap import LinearColormap
import geopandas as gpd
import pandas as pd
import numpy as np
import mapclassify
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss


import matplotlib.pyplot as plt
import data_analysis.compare_scheduled_and_rt as compare_scheduled_and_rt
import data_analysis.static_gtfs_analysis as static_gtfs_analysis
import plotly.express as px
import plotly.graph_objects as go
import chart_studio.plotly as py


CHICAGO_COORDINATES = (41.85, -87.68)

# Return the project root directory
# https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
project_name = os.getenv('PROJECT_NAME', 'chn-ghost-buses')
current_dir = Path(__file__)
project_dir = next(
    p for p in current_dir.parents
    if p.name == f'{project_name}'
)
PLOTS_PATH = project_dir / 'plots' / 'scratch'
DATA_PATH = project_dir / 'data_output' / 'scratch'

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


# https://stackoverflow.com/questions/52503899/
# format-round-numerical-legend-label-in-geopandas
def legend_formatter(
    df: gpd.GeoDataFrame,
    var: str,
        decimals: str = '0f') -> List[str]:
    """Format the bounds for categorical variable to remove brackets
        and add comma separators for large numbers

    Args:
        df (gpd.GeoDataFrame): data containing routes and GPS coordinates
        var (str): variable of interest to be plotted.
        decimals (str, optional): Number of decimal places 
            for the number display. Defaults to 0f (0 places).

    Returns:
        List[str]: A list of formatted bounds to be placed in legend.
    """
    df = df.copy()
    q5 = mapclassify.Quantiles(df[var], k=5)
    upper_bounds = q5.bins
    bounds = []
    for index, upper_bound in enumerate(upper_bounds):
        if index == 0:
            lower_bound = df[var].min()
        else:
            lower_bound = upper_bounds[index-1]
        if np.isnan(upper_bound):
            upper_bound = df[var].max()

        # format the numerical legend here
        bound = f'{lower_bound:,.{decimals}} - {upper_bound:,.{decimals}}'
        bounds.append(bound)
    return bounds


def n_worst_best_routes(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    n: int = 10,
    percentile: bool = True,
        worst: bool = True) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns the n route_ids with the lowest or highest ratio
       of completed trips

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): DataFrame with ratio
            of completed trips by route_id
        n (int, optional): number of route_ids to return . Defaults to 10.
        percentile (bool, optional): whether to return routes at a
            certain percentile rank. Defaults to True.
        worst (bool, optional): whether to return the lowest ratios.
            Defaults to True.
    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: A DataFrame containing the
            n worst or best routes.
    """
    df = df.copy()
    if percentile:
        if n > 100 or n <= 0:
            raise ValueError("The accepted values for n are 0 < n <= 100")

    if worst:
        if percentile:
            return (
                df.loc[df['percentiles'] <= n/100]
            )
        else:
            # Drop duplicates so that there are n unique routes
            # instead of duplicated routes when taking direction into account.
            return (
                df.sort_values(by="ratio")
                .drop_duplicates('route_id').head(n)
            )
    else:
        if percentile:
            return (
                df.loc[df['percentiles'] >= (100-n)/100]
            )
        else:
            return (
                df.sort_values(by="ratio", ascending=False)
                .drop_duplicates('route_id').head(n)
            )


def create_save_path(save_name: str, dir_name: Path = PLOTS_PATH) -> str:
    """Generate a save path for files

    Args:
        save_name (str): the name of the file to save
        dir_name (Path, optional): the directory in which to save the file.
            Defaults to PLOTS_PATH.

    Returns:
        str: A full path with the directory and filename
    """
    return str(
        dir_name / f'{save_name}'
        f'_{datetime.now().strftime("%Y-%m-%d")}'
    )


def lineplot(
        x: str,
        y: str,
        data: pd.DataFrame,
        xlabel: str = None,
        ylabel: str = None,
        color_var: str = None,
        show: bool = False,
        title: str = None,
        save: bool = True,
        save_name: str = None,
        rolling_median: bool = False) -> None:
    """Plot a line plot using Plotly

    Args:
        x (str): x-axis variable
        y (str): y-axis variable
        data (pd.DataFrame): DataFrame for plot
        xlabel (str, optional): x-axis label. Defaults to None.
        ylabel (str, optional): y-axis label. Defaults to None.
        color_var (str, optional): variable to color lines. Defaults to None.
        show (bool, optional): whether to display plot in interactive session.
            Defaults to False.
        title (str, optional): plot title. Defaults to None.
        save (bool, optional): whether to save plot. Defaults to True.
        save_name (str, optional): name of saved plot file. Defaults to None.
        rolling_median (bool, optional): whether to add a line showing
            the rolling median. Defaults to False.
    """

    data = data.copy()
    fig = go.Figure()
    if color_var is not None:
        if color_var == 'day_type':
            labels = {}
            labels[color_var] = "Day Type"
            labels['wk'] = 'Weekday'
            labels['hol'] = 'Holiday'
            labels['sat'] = 'Saturday'
            labels['sun'] = 'Sunday'

            for day_type in data[color_var].unique():
                sub_df = data.loc[data[color_var] == day_type]
                x1 = sub_df[x].values
                y1 = sub_df[y].values

                # Use markers only for holidays and line+markers for weekends
                if day_type == 'hol':
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type],
                        mode='markers'
                    ))
                elif day_type == 'sat' or day_type == 'sun':
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type],
                        mode='lines+markers'
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type]
                    ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel,
                legend_title="Day Type"
            )
        else:
            for val in data[color_var].unique():
                sub_df = data.loc[data[color_var] == val]
                x1 = sub_df[x].values
                y1 = sub_df[y].values

                fig.add_trace(go.Scatter(
                    x=x1,
                    y=y1,
                    name=val
                ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel
            )
    else:
        x1 = data[x].values
        y1 = data[y].values

        fig.add_trace(go.Scatter(
            x=x1,
            y=y1,
            showlegend=False
        ))
        if rolling_median:
            rolling_m = data.set_index(x)[y].rolling('7D').median().values
            fig.add_trace(go.Scatter(
                x=x1,
                y=rolling_m,
                name='Rolling 7-day median'
            ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel,
                showlegend=True
            )

    if show:
        fig.show()

    if save:
        if save_name is None:
            save_name = f'lineplot_of_{y}_over_{x}'
        save_path = create_save_path(save_name)
        logger.info(f'Saving to {save_path}')
        fig.write_html(f'{save_path}.html')
        fig.write_image(f'{save_path}.png')
        logger.info('Saving to Chart Studio')
        py.plot(fig, filename=f"{save_name}")


def boxplot(
        x: str,
        y: str,
        data: pd.DataFrame,
        xlabel: str = None,
        ylabel: str = None,
        show: bool = False,
        title: str = None,
        save: bool = True,
        save_name: str = None) -> None:
    """ Make a boxplot and save the figure

    Args:
        x (str): the variable to plot by
        y (str): the variable of interest e.g. ratio
        data (pd.DataFrame): DataFrame to plot with
        xlabel (str, optional): Label for x axis. Defaults to None.
        ylabel (str, optional): Label for y axis. Defaults to None.
        title (str, optional): Plot title. Defaults to None
        show (bool, optional): whether to show plot. Defaults to False.
        save (bool, optional): whether to save plot. Defaults to True.
        save_name (str, optional): Name of the saved boxplot. Defaults to None.
    """
    data = data.copy()
    if x == 'day_type':
        rename_dict = {
            'wk': 'weekday',
            'hol': 'holiday',
            'sat': 'saturday',
            'sun': 'sunday'
        }
        data.replace({x: rename_dict}, inplace=True)
    labels = {}
    if xlabel is not None:
        labels[x] = xlabel
        plt.xlabel(xlabel)
    if ylabel is not None:
        labels[y] = ylabel
    fig = px.box(
        data_frame=data,
        x=x,
        y=y,
        title=title,
        labels=labels
    )
    if show:
        fig.show()

    if save:
        if save_name is None:
            save_name = f'boxplot_of_{y}_by_{x}'
        save_path = create_save_path(save_name)
        logger.info(f'Saving to {save_path}')
        fig.write_html(f'{save_path}.html')
        fig.write_image(f'{save_path}.png')
        logger.info('Saving to Chart Studio')
        py.plot(fig, filename=f"{save_name}")


def plot_map(
    geo_df: gpd.GeoDataFrame,
    save_name: str,
    save: bool = True,
        **kwargs: dict) -> folium.Map:
    """ Create a map of bus routes from GeoDataFrame

    Args:
        geo_df (gpd.GeoDataFrame): DataFrame with bus routes, GPS coordinates,
           and some variable of interest e.g. ratio
        save_name (str): The name of the saved output map.
        save (bool, optional): Whether to save the map. Defaults to True.
        **kwargs (dict): A dictionary of keyword arguments passed
            to GeoDataFrame.explore
    Returns:
        folium.Map: A map of bus routes colored by a target variable.
    """
    geo_df = geo_df.copy()
    # Convert dates to strings. Folium cannot handle datetime
    date_columns = geo_df.select_dtypes(include=["datetime64"]).columns
    geo_df[date_columns] = geo_df[date_columns].astype(str)
    newmap = geo_df.explore(**kwargs)
    if save:
        save_path = create_save_path(f'{save_name}_{kwargs["column"]}')
        logger.info(f'Saving to {save_path}')
        newmap.save(f"{save_path}.html")
    return newmap


def create_ridership_map(mvp: bool = False) -> None:
    """Generate a map of ridership per route

    Args:
        mvp (bool, optional): Whether to generate the data and map
            used in the MVP. Defaults to False.
    """
    # Daily totals by route
    ridership_by_rte_date = pd.read_csv(
        "https://data.cityofchicago.org/api/views/"
        "jyb9-n7fm/rows.csv?accessType=DOWNLOAD")

    ridership_by_rte_date['date'] = pd.to_datetime(
        ridership_by_rte_date['date'],
        infer_datetime_format=True
    )

    if mvp:
        # Date range from 2022-05-20 to 2022-07-31
        ridership_by_rte_date = (
            ridership_by_rte_date.loc[
                (ridership_by_rte_date['date'] >= "2022-05-20")
                & (ridership_by_rte_date['date'] <= "2022-07-31")
            ]
        )

    start_date = ridership_by_rte_date['date'].min()
    end_date = ridership_by_rte_date['date'].max()

    # Total number of riders per route
    ridership_by_rte = (
        ridership_by_rte_date
        .groupby('route')
        .sum()
        .reset_index()
    )

    ridership_by_rte.rename(columns={'route': 'route_id'}, inplace=True)

    gdf = static_gtfs_analysis.main()

    rider_gdf = ridership_by_rte.merge(gdf, on="route_id")

    rider_gdf_geo = gpd.GeoDataFrame(rider_gdf)

    # Background map must be re-created to get a clear map between runs
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    bounds = legend_formatter(rider_gdf_geo, "rides")
    kwargs = {
        "cmap": "plasma",
        "column": "rides",
        "scheme": "Quantiles",
        "m": chicago_map,
        "legend_kwds": {
            'caption': 'Number of Riders',
            'colorbar': False,
            'labels': bounds
        },
        "legend": True,
        "categorical": False,
        "k": 5
    }

    _ = plot_map(
        rider_gdf_geo,
        save_name=f'all_routes_quantiles_{start_date}_to_{end_date}',
        **kwargs
    )

    # Remove key from kwargs
    kwargs.pop('scheme', None)
    kwargs.pop('k', None)
    kwargs['legend_kwds'].pop('labels', None)
    # Dictionary keys are popped when passed to explore,
    # so must be added again.
    kwargs['legend_kwds']['caption'] = 'Number of Riders'
    kwargs['legend_kwds']['colorbar'] = False
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    kwargs['m'] = chicago_map

    plotted_map = plot_map(
        rider_gdf_geo,
        save=False,
        save_name='',
        **kwargs
    )
    # Manually specify plasma colormap to avoid overlapping labels
    # See https://www.kennethmoreland.com/color-advice/
    # :~:text=Plasma,der%20Walt%20and%20Nathaniel%20Smith.
    kwargs['legend_kwds']['caption'] = 'Number of Riders'
    kwargs['legend_kwds']['colorbar'] = False
    lcm = LinearColormap(
        colors=[
            (13, 8, 135),
            (84, 2, 163),
            (139, 10, 165),
            (185, 50, 137),
            (219, 92, 104),
            (244, 136, 73),
            (254, 188, 43),
            (240, 249, 33)
        ],
        vmin=0,
        vmax=rider_gdf_geo[kwargs["column"]].max(),
        caption=kwargs["legend_kwds"]["caption"]
    )

    lcm.add_to(plotted_map)

    save_name = f'all_routes_numeric_{start_date}_to_{end_date}'

    save_path = create_save_path(f'{save_name}_{kwargs["column"]}')
    plotted_map.save(f"{save_path}.html")


def groupby_long_df(df: pd.DataFrame,
                    groupbyvars: Union[List[str], str]) -> pd.DataFrame:
    """Group DataFrame by groupbyvars

    Args:
        df (pd.DataFrame): combined_long output DataFrame from
            compare_scheduled_and_rt.main
        groupbyvars Union[List[str], str]: A column or list
            of columns to group by.

    Returns:
        pd.DataFrame: A DataFrame grouped by groupby
            vars and a trip ratio column.
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])

    df = (
        df.groupby(groupbyvars)[['trip_count_rt', 'trip_count_sched']]
        .sum()
        .reset_index()
    )

    df['ratio'] = (
        df['trip_count_rt']
        / df['trip_count_sched']
    )

    return df


def calculate_percentile_and_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Add a percentile and rank column to the DataFrame

    Args:
        df (pd.DataFrame): A summary DataFrame output from
            compare_scheduled_and_rt.main

    Returns:
        pd.DataFrame: A summary DataFrame with the columns
            percentiles and rank added.
    """
    df = df.copy()
    df['percentiles'] = df['ratio'].rank(
        pct=True
    )
    df['ranking'] = (
        df['ratio']
        .rank(method='dense', na_option='top', ascending=False)
    )
    return df


def main(mvp: bool = False) -> None:
    """Generate boxplot, maps of all routes, top 10 best routes, and
    top 10 worst routes. Map of ridership

    Args:
        mvp (bool, optional): whether to generate plots and data
            used in the MVP. Defaults to False.
    """
    logger.info("Creating GeoDataFrame")
    gdf = static_gtfs_analysis.main()

    logger.info("Getting latest real-time and schedule comparison data")

    if mvp:
        # Date range is between 2022-05-20 and 2022-11-05
        logger.info(
            "Loading data for MVP."
            " Date range is between 2022-05-20 and 2022-11-05"
        )
        try:
            combined_long_df = pd.read_csv(
                DATA_PATH / 'combined_long_df_2022-11-06.csv'
            )
            summary_df = pd.read_csv(DATA_PATH / 'summary_df_2022-11-06.csv')
        except FileNotFoundError:
            combined_long_df = pd.read_csv(
                DATA_PATH.parent / 'combined_long_df_2022-11-06.csv'
            )
            summary_df = pd.read_csv(
                DATA_PATH.parent / 'summary_df_2022-11-06.csv'
            )

        # MVP is weekday only
        logger.info("Keeping only weekday data for MVP")
        summary_df_wk = summary_df.loc[summary_df.day_type == 'wk']
        summary_df_wk = calculate_percentile_and_rank(summary_df_wk)

        summary_gdf = summary_df_wk.merge(gdf, how="right", on="route_id")

    else:
        combined_long_df, summary_df = compare_scheduled_and_rt.main(freq='D')
        summary_df = calculate_percentile_and_rank(summary_df)

        logger.info("Saving data")
        combined_long_path = create_save_path('combined_long_df', DATA_PATH)
        combined_long_df.to_csv(f'{combined_long_path}.csv', index=False)
        summary_df_path = create_save_path('summary_df', DATA_PATH)
        summary_df.to_csv(f'{summary_df_path}.csv', index=False)

        summary_gdf = summary_df.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    combined_long_df['date'] = pd.to_datetime(combined_long_df['date'])
    start_date = combined_long_df['date'].min().strftime('%Y-%m-%d')
    end_date = combined_long_df['date'].max().strftime('%Y-%m-%d')

    logger.info("Creating map of all routes")
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    summary_kwargs = {
        "cmap": "plasma",
        "column": "ratio",
        "m": chicago_map,
        "legend_kwds": {
            "caption": "Ratio of Actual Trips to Scheduled Trips",
            "max_labels": 5},
        "legend": True,
    }
    save_name = f"all_routes_{start_date}_to_{end_date}"
    if mvp:
        save_name += "_wk"
    else:
        save_name += "_all_day_types"
    _ = plot_map(
            summary_gdf_geo,
            save_name=save_name,
            **summary_kwargs
        )

    all_routes_path = create_save_path(save_name, DATA_PATH)
    summary_gdf_geo.to_file(f'{all_routes_path}.json', driver='GeoJSON')

    # Quantile plot
    logger.info("Creating map of all routes binned by quintile")
    bounds = legend_formatter(summary_gdf_geo, "ratio", decimals="2f")
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    summary_kwargs_quantiles = {
        "cmap": "plasma",
        "column": "ratio",
        "scheme": "Quantiles",
        "m": chicago_map,
        "legend_kwds": {
            'caption': "Ratio of Actual Trips to Scheduled Trips",
            'colorbar': False,
            'labels': bounds
        },
        "legend": True,
        "categorical": False,
        "k": 5
    }
    save_name = f"all_routes_quantiles_{start_date}_to_{end_date}"
    if mvp:
        save_name += "_wk"
    else:
        save_name += "_all_day_types"
    _ = plot_map(
            summary_gdf_geo,
            save_name=save_name,
            **summary_kwargs_quantiles
        )

    # Worst performing routes
    logger.info("Creating map of worst performing routes")
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    worst_geo = n_worst_best_routes(summary_gdf_geo, percentile=False)

    save_name = f"worst_routes_{start_date}_to_{end_date}"
    if mvp:
        save_name += "_wk"
    else:
        save_name += "_all_day_types"

    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['cmap'] = 'winter'
    summary_kwargs['m'] = chicago_map

    _ = plot_map(
            worst_geo,
            save_name=save_name,
            **summary_kwargs
        )
    worst_geo_path = create_save_path(save_name, DATA_PATH)
    worst_geo.to_file(f'{worst_geo_path}.json', driver="GeoJSON")

    # Best routes
    logger.info("Creating map of best performing routes")
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    best_geo = n_worst_best_routes(
        summary_gdf_geo,
        percentile=False,
        worst=False
    )
    save_name = f"best_routes_{start_date}_to_{end_date}"
    if mvp:
        save_name += "_wk"
    else:
        save_name += "_all_day_types"


    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['m'] = chicago_map
    _ = plot_map(
            best_geo,
            save_name=save_name,
            **summary_kwargs
        )
    best_geo_path = create_save_path(save_name, DATA_PATH)
    best_geo.to_file(f'{best_geo_path}.json', driver="GeoJSON")

    # Other plots
    combined_long_groupby_date = groupby_long_df(
        combined_long_df,
        'date'
    )

    combined_long_groupby_day_type = groupby_long_df(
        combined_long_df,
        ['date', 'day_type']
    )

    logger.info(
        f"Result of the ADF test is"
        f" {adfuller(combined_long_groupby_date['ratio'])}"
    )
    # p-value is 0.95 > 0.05. Fail to reject the null of non-stationarity.
    logger.info(
        f"Result of the KPSS test is"
        f" {kpss(combined_long_groupby_date['ratio'],regression='c', nlags='auto')}"
    )
    # p-value is 0.01 < 0.05. Reject the null hypothesis of stationarity.

    # Both tests indicate that the time series is probably non-stationary.

    logger.info("Creating lineplot with moving median")
    lineplot(
        data=combined_long_groupby_date,
        x='date',
        y='ratio',
        xlabel='Date',
        ylabel='Ratio of actual trips to scheduled trips',
        rolling_median=True,
        title=f'Trip ratios<br>{start_date} to {end_date}',
        save_name="lineplot_of_ratio_over_time_w_rolling_median"
    )

    logger.info("Creating line plot by day type")
    lineplot(
        data=combined_long_groupby_day_type,
        x="date",
        y="ratio",
        xlabel="Date",
        ylabel="Ratio of actual trips to scheduled trips",
        color_var="day_type",
        title=f"Trip ratios by Day Type<br>{start_date} to {end_date}",
        save_name="lineplot_of_ratio_over_time_by_day_type"
    )

    logger.info("Creating box plot by day type")
    boxplot(
        x="day_type",
        y="ratio",
        data=summary_df,
        xlabel="Day Type",
        ylabel="Ratio of actual trips to scheduled trips",
        title=f"Trip ratio distribution by Day Type<br>"
              f"{start_date} to {end_date}"
    )


if __name__ == '__main__':
    main()
