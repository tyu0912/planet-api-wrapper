import requests

from requests.auth import HTTPBasicAuth

auth = HTTPBasicAuth(PLANET_API_KEY, '')
HEADERS = {'content-type': 'application/json'}

def prepare_data(coordinates):
    waypoint_df = pd.DataFrame(waypoint_data)
    waypoint_gdf = gpd.GeoDataFrame(waypoint_df,
                                    geometry=gpd.points_from_xy(waypoint_df.longitude, waypoint_df.latitude))

    # Applying WGS84 to the CRS
    waypoint_gdf.crs = {'init': 'epsg:4326'}

    # # Prep GeoDataFrame with buffers
    #
    # Converting geodataframe to Meters from Lat/Long
    # Allows for square buffer to be applied (450m)
    point_gdf_m = waypoint_gdf.to_crs(epsg=3395)

    # Applying the buffer, cap_style = 3 --> Square Buffer, 383.5 = 256x256 chip size
    buffer = point_gdf_m.buffer(383.5, cap_style=3)

    # # Convert buffer back to WGS84 Lat/Long
    buffer_wgs84 = buffer.to_crs(epsg=4326)

    # Merging GDF and DF to get the Waypoint names
    joined_buffer_wgs84 = pd.concat([waypoint_df, buffer_wgs84], axis=1)
    joined_buffer_wgs84 = joined_buffer_wgs84.rename(columns={0: 'polygon'}).set_geometry('polygon')

    joined_buffer_wgs84_drop = joined_buffer_wgs84.drop(['geometry'], axis=1)
    joined_buffer_wgs84_json = joined_buffer_wgs84_drop.to_json()

    # transforming to json for inclusion into Planet API
    buffer_wgs84_json_parsed = json.loads(joined_buffer_wgs84_json)
    buffer_wgs84_json_api = buffer_wgs84_json_parsed['features']  # [0]['geometry']['coordinates']

    return buffer_wgs84_json_api


def build_order(index):

    # get images that overlap with our AOI
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": {
            "type": "Point",
            "coordinates": [index['properties']['longitude'], index['properties']['latitude']]
        }
    }

    date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
            "gte": start_date,
            "lte": today
        }
    }

    # only get images which have <10% cloud coverage
    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
            "lte": 0.1
        }
    }

    # combine our geo, date, cloud filters
    combined_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }

    # API request object
    search_request = {
        "interval": "day",
        "item_types": ["PSScene4Band"],
        "asset_types": "analytic_sr",
        "filter": combined_filter
    }

    # Another function that's here: 'https://api.planet.com/data/v2'

    search_result = requests.post('https://api.planet.com/data/v1/quick-search',
                                  auth=HTTPBasicAuth(PLANET_API_KEY, ''),
                                  json=search_request
                                  )

    return search_result


def poll_for_success(order_url, auth, num_loops=100, sleep_time=10):
    count = 0
    state = ''

    while state not in ['success', 'partial', 'failed']:
        if count > 0:
            time.sleep(sleep_time)  # used to rate limit requests
        #             print(f'{(count - 1) * sleep_time}: {state}')

        r = requests.get(order_url, auth=auth)
        count += 1

        try:
            response = r.json()
        except:
            continue

        state = response['state']

    return response


def place_order(request, auth, sleep_time=3):
    ORDERS_V2_URL = 'https://api.planet.com/compute/ops/orders/v2'
    count = 0

    while True:
        try:
            response = requests.post(ORDERS_V2_URL, data=json.dumps(request), auth=auth, headers=HEADERS)
            order_id = response.json()['id']
            order_url = ORDERS_V2_URL + '/' + order_id
            break
        except:
            time.sleep(sleep_time)  # used to rate limit requests
            count = count + 1
            #             print(f'{(count - 1) * sleep_time}')
            continue

    return order_url


def download_order(waypoint_row, auth, overwrite=False):
    # set up options for conversion to jpg
    gdal_translate_options_list = [
        '-ot Byte',
        '-of JPEG',
        '-b 1',
        '-b 2',
        '-b 3',
        '-b 4',
        '-scale min_val max_val'
    ]
    gdal_translate_options_string = " ".join(gdal_translate_options_list)

    print(waypoint_row)
    c = 0
    while True:
        try:
            response = poll_for_success(waypoint_row['order_url'], auth=auth)
            break
        except:
            print(c)
            time.sleep(20)
            c = c + 1

    print(response['state'])

    if response['state'] in ['success', 'partial']:

        results = response['_links']['results']
        results_urls = [r['location'] for r in results if '_3B_AnalyticMS_SR_clip.tif' in r['name']][0]
        results_names = [r['name'] for r in results if '_3B_AnalyticMS_SR_clip.tif' in r['name']][0]

        results_local_tif_path = pathlib.Path(os.path.join('data', waypoint_row['lat_lon_name'],
                                                           f"{waypoint_row['Image_ID']}_3B_AnalyticMS_SR_clip.tif"))
        results_local_jpg_path = pathlib.Path(os.path.join('data', waypoint_row['lat_lon_name'],
                                                           f"{waypoint_row['Image_ID']}_3B_AnalyticMS_SR_clip.jpg"))
        results_s3_tif_path = get_s3_key_for_image(waypoint_row, extension='tif')
        results_s3_jpg_path = get_s3_key_for_image(waypoint_row, extension='jpg')

        if overwrite or not results_local_tif_path.exists():
            print(f'downloading {results_names} to {results_local_tif_path}')
            r = requests.get(results_urls, allow_redirects=True)
            results_local_tif_path.parent.mkdir(parents=True, exist_ok=True)
            open(results_local_tif_path, 'wb').write(r.content)

            local_tif_file = os.path.relpath(results_local_tif_path)
            s3_tif_file = os.path.relpath(results_s3_tif_path)

            upload_to_aws(local_tif_file,
                          S3_BUCKET,
                          s3_tif_file)

            local_jpg_file = os.path.relpath(results_local_jpg_path)
            s3_jpg_file = os.path.relpath(results_s3_jpg_path)
            print(f'S3 Jpg file: {s3_jpg_file}')

            gdal.Translate(local_jpg_file,
                           local_tif_file,
                           options=gdal_translate_options_string)

            upload_to_aws(local_jpg_file,
                          S3_BUCKET,
                          s3_jpg_file)

            print(f'Bucket: {S3_BUCKET}')
            print(f'key: {s3_jpg_file}')
            print(f'S3 Jpg exists: {s3_object_exists(Bucket=S3_BUCKET, Key=s3_jpg_file)}')

    #                 # Remove temp files
    #                 remove(local_jpg_file)
    #                 remove(s3_jpg_file)

    else:
        print('download_failed')
        results_s3_tif_path = 'download_failed'

    return results_s3_tif_path


def planet_api_pull(waypoint_row, overwrite=False):
    results_s3_path = get_s3_key_for_image(waypoint_row)
    print(results_s3_path)

    if overwrite or not s3_object_exists(Bucket=S3_BUCKET, Key=results_s3_path):

        # Creating the URLs to activate the images...prevents latency during download
        waypoint_row['id0_url'] = f"https://api.planet.com/data/v1/item-types/{item_type}/items/{waypoint_row['Image_ID']}/assets"

        # Returns JSON metadata for assets in this ID.
        # Learn more: planet.com/docs/reference/data-api/items-assets/#asset
        waypoint_row['activation_link'] = requests.get(
            waypoint_row['id0_url'],  # link
            auth=HTTPBasicAuth(PLANET_API_KEY, '')
        )

        # Getting Result Links
        waypoint_row['links'] = waypoint_row['activation_link'].json()[u"analytic_sr"]["_links"]

        # Generating a list of activation links
        waypoint_row['activation_link'] = waypoint_row['links']["activate"]

        # Request activation of the 'visual' asset:
        # for a in joined_buffer_wgs84_drop_merge['activation_link']:
        activate_result = requests.get(waypoint_row['activation_link'], auth=HTTPBasicAuth(PLANET_API_KEY, ''))

        # Building the order lists starting with the product information
        waypoint_row['single_product'] = [{'item_ids': [waypoint_row['Image_ID']],'item_type': 'PSScene4Band','product_bundle': 'analytic_sr'}]

        # Setting the clipping boundaries
        waypoint_row['clip'] = [{
            'clip': {
                'aoi': {
                    'type': 'Polygon',
                    'coordinates': [waypoint_row['poly_list']]
                }
            }
        }]

        # create an order request with the clipping tool
        waypoint_row['request_clip'] = {
            'name': 'just clip',
            'products': waypoint_row['single_product'],  # single_product,
            'tools': waypoint_row['clip']
        }

        print('Placing Order')
        waypoint_row['order_url'] = place_order(waypoint_row['request_clip'], auth)

        print('Downloading Order')
        waypoint_row['results_s3_path'] = download_order(waypoint_row, auth, overwrite)

    else:
        # print(f'{s3_tif_path} already exists, skipping {s3_tif_path}')
        waypoint_row['results_s3_path'] = [results_s3_path]

    print(waypoint_row)
    return waypoint_row