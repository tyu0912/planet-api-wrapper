import pandas as pd
import geopandas as gpd
import json
import time
import os
import numpy as np
import requests
import pathlib
import boto3
import argparse

from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from osgeo import gdal
from app.resource import utils

parser = argparse.ArgumentParser()

parser.add_argument("--planet_key", required=False, default="d1522b0573f548ea8233d09b9a917b74")
parser.add_argument("--aws_key", required=False, default="AKIA2XFXMY3RHQI3XHOP")
parser.add_argument("--aws_secret", required=False, default="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")
parser.add_argument("--aws_bucket", required=False, default="w210-planet-data-api")

args = parser.parse_args()

if __name__ == "__main__":

    waypoint_data = {
        'Waypoint': ['Orangi River'],
        'latitude': [-2.302313],
        'longitude': [34.830777]
    }

    today = datetime.isoformat(datetime.utcnow()) + 'Z'  # (datetime.today())
    start_date = datetime.isoformat(datetime.utcnow() - timedelta(7)) + 'Z'

    # Getting Image ID's for each waypoint that has the analytic_sr dataset
    # Having to ping the Planet V1 API to return the image id's for our required filter
    # Filter variables include: Center Coordinate, Date Range, Cloud Cover, Item Type and Asset Type

    id_list = []

    for index in buffer_wgs84_json_api:
        waypoint = index["properties"]["Waypoint"]
        order = build_order(index)

        time.sleep(3)
        order = order.json()['features']

        # appending Image ID to `joined_buffer_wgs84_drop_merge` if the analytic_sr is available
        # Will only return image id's that meet this requirement.
        for i in order:
            # print(order)
            if "assets.analytic_sr:download" in i["_permissions"]:
                id_list.append((waypoint, i["id"], i["properties"]))


    # Merging image id's to the dataframe to maintain continuity

    image_ids = pd.DataFrame(np.asarray(id_list))
    image_ids.rename(columns={0: 'Waypoint', 1: 'Image_ID', 2: 'Image_Properties'}, inplace=True)
    image_ids = pd.concat([image_ids.drop(['Image_Properties'], axis=1), pd.json_normalize(image_ids['Image_Properties'])],
                          axis=1)
    joined_buffer_wgs84_drop_merge = pd.merge(joined_buffer_wgs84_drop, image_ids, on='Waypoint')

    # Converting list of tuple polygons to list of lists polygons
    # This step is necessary to pull the Geometry from `joined_buffer_wgs84_drop_merge`
    # and convert to a list of lists...appending to `joined_buffer_wgs84_drop_merge`.


    coordinates = joined_buffer_wgs84_drop_merge.polygon.apply(lambda geom: list(geom.exterior.coords))

    res = []
    for poly in coordinates:
        res_2 = list(map(list, poly))
        res.append(res_2)

    joined_buffer_wgs84_drop_merge['poly_list'] = res
    joined_buffer_wgs84_drop_merge['lat_lon_name'] = 'lat_' + joined_buffer_wgs84_drop_merge.latitude.map(str) + '_long_' + joined_buffer_wgs84_drop_merge.longitude.map(str)
    joined_buffer_wgs84_drop_merge = joined_buffer_wgs84_drop_merge.sort_values(['lat_lon_name', 'updated']).drop_duplicates('lat_lon_name', keep='last').sort_index()


    # For Loop
    results_df = pd.DataFrame()

    for index, waypoint_row in joined_buffer_wgs84_drop_merge.iterrows():
        row = planet_api_pull(waypoint_row, overwrite=False)
        results_df = results_df.append(row)


    results_df[['Waypoint', 'latitude', 'longitude', 'acquired', 'results_s3_path']].to_csv(
        pathlib.Path(os.path.join('data/results.csv')))

    results_df.to_csv(pathlib.Path(os.path.join('data/results_all_cols.csv')))