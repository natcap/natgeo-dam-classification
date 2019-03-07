"""Create a sentinel sqlite database index."""
import threading
import queue
import json
from lxml import etree
import subprocess
import time
import csv
import gzip
import zipfile
import shutil
import sys
import os
import logging
import sqlite3
import re
import requests

import math
import numpy
import urllib.request
import urllib.error
from osgeo import gdal
from osgeo import osr
import reproduce
import pygeoprocessing
import taskgraph

import data_validator

OPENER = urllib.request.build_opener()
OPENER.addheaders = [('User-agent', 'Mozilla/5.0')]
urllib.request.install_opener(OPENER)

WORKSPACE_DIR = 'workspace_imagery'
IMAGE_DIR = 'images'
DAM_IMAGERY_DIR = os.path.join(IMAGE_DIR, 'dam_imagery')
BOUNDING_BOX_DB_PATH = os.path.join(IMAGE_DIR, 'bounding_box_image_db.db')
SENTINEL_CSV_BUCKET_ID_TUPLE = ('gcp-public-data-sentinel-2', 'index.csv.gz')
SENTINEL_CSV_GZ_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.csv.gz')
SENTINEL_SQLITE_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.db')
IAM_TOKEN_PATH = 'ecoshard-202992-key.json'
SEARCH_BLOCK_SIZE = 500
MIN_SURFACE_WATER = 20
MAX_SURFACE_WATER = 80
REPORTING_INTERVAL = 5.0
POLL_RATE = 1.0  # wait this many seconds for polling changes in the database
LOGGER = logging.getLogger(__name__)


def build_index(task_graph):
    """Build database backend for Sentinel imagery.

    Parameters:
        task_graph (TaskGraph): taskgraph for global scheduling.

    Returns:
        None.

    """
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass

    download_index_task = task_graph.add_task(
        func=reproduce.utils.google_bucket_fetch,
        args=(
            SENTINEL_CSV_BUCKET_ID_TUPLE[0], SENTINEL_CSV_BUCKET_ID_TUPLE[1],
            IAM_TOKEN_PATH, SENTINEL_CSV_GZ_PATH),
        target_path_list=[SENTINEL_CSV_GZ_PATH],
        task_name=f'fetch sentinel index')

    _ = task_graph.add_task(
        func=gzip_csv_to_sqlite,
        args=(SENTINEL_CSV_GZ_PATH, SENTINEL_SQLITE_PATH),
        target_path_list=[SENTINEL_SQLITE_PATH],
        dependent_task_list=[download_index_task],
        task_name='unzip index csv')

    task_graph.join()


def get_dam_bounding_box_imagery_planet(
        task_graph, dam_id, bounding_box, workspace_dir,
        planet_asset_fetch_queue):
    try:
        os.makedirs(workspace_dir)
    except OSError:
        pass
    aoi = {
        "type": "Polygon",
        "coordinates": [
            [
                [bounding_box[0], bounding_box[1]],
                [bounding_box[2], bounding_box[1]],
                [bounding_box[2], bounding_box[3]],
                [bounding_box[0], bounding_box[3]],
                [bounding_box[0], bounding_box[1]],
            ]
        ]
    }
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": aoi
    }

    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
            "lte": 0.0
        }
    }

    bounding_box_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, cloud_cover_filter]
    }

    # Stats API request object
    stats_endpoint_request = {
        "item_types": ["REOrthoTile"],
        "filter": bounding_box_filter
    }

    session = requests.Session()
    session.auth = (os.environ['PL_API_KEY'], '')

    while True:
        try:
            search_result_json = session.post(
                'https://api.planet.com/data/v1/quick-search',
                json=stats_endpoint_request,
                timeout=5).json()
        except requests.Timeout:
            LOGGER.exception('trying again')

    for feature in search_result_json['features']:
        try:
            asset_url = (
                f'https://api.planet.com/data/v1/item-types/'
                f"REOrthoTile/items/{feature['id']}/assets")
            asset_result_json = session.get(asset_url, timeout=5).json()
            if 'visual' in asset_result_json:
                item_activation_url = (
                    asset_result_json['visual']['_links']['activate'])
                session.post(item_activation_url, timeout=5)
                # request activation
                granule_path = os.path.join(
                    workspace_dir, f"{feature['id']}.tif")
                LOGGER.info("scheduling download of %s", feature['id'])
                planet_asset_fetch_queue.put(
                    (granule_path, f"{feature['id']}_{dam_id}", bounding_box,
                     asset_url))
                break
        except requests.Timeout:
            LOGGER.exception('trying again')


def process_planet_asset_fetch_queue(
        planet_asset_fetch_queue, dam_imagery_dir, workspace_dir):
    for path in [dam_imagery_dir, workspace_dir]:
        try:
            os.makedirs(path)
        except OSError:
            pass
    session = requests.Session()
    session.auth = (os.environ['PL_API_KEY'], '')
    processing_queue = queue.Queue()
    should_stop = False
    while True:
        time.sleep(1)
        try:
            payload = planet_asset_fetch_queue.get_nowait()
            if payload == 'STOP':
                should_stop = True
                continue
        except queue.Empty:
            try:
                payload = processing_queue.get_nowait()
            except queue.Empty:
                if should_stop:
                    return
                continue
        try:
            granule_path, feature_id, bounding_box, asset_url = payload
            LOGGER.debug('got a payload %s', payload)
            asset_result_json = session.get(
                asset_url, timeout=5.0).json()
            asset_status = asset_result_json['visual']['status']
            LOGGER.debug('asset status %s', asset_status)
            if asset_status != 'active':
                LOGGER.info('not active, rescheduling')
                processing_queue.put(payload)
                continue
            granule_url = asset_result_json['visual']['location']
            granule_path = os.path.join(workspace_dir, f"{feature_id}.tif")
            if not os.path.exists(granule_path):
                LOGGER.debug('retrieve %s', granule_path)
                urllib.request.urlretrieve(
                    granule_url, granule_path)
        except requests.timeout:
            LOGGER.exception('trying again')
            processing_queue.put(payload)
            continue

        png_path = os.path.join(workspace_dir, f"{feature_id}_clip.png")
        target_png_path = os.path.join(
            dam_imagery_dir, os.path.basename(png_path))
        if not os.path.exists(png_path):
            granule_raster_info = pygeoprocessing.get_raster_info(granule_path)
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)
            target_bounding_box = pygeoprocessing.transform_bounding_box(
                bounding_box, wgs84_srs.ExportToWkt(),
                granule_raster_info['projection'], edge_samples=11)

            lat_len_deg = abs(target_bounding_box[1]-target_bounding_box[3])
            lng_len_deg = abs(target_bounding_box[0]-target_bounding_box[2])
            center_lat = (target_bounding_box[1]+target_bounding_box[3])/2

            lat_d_to_m, lng_d_to_m = len_of_deg_to_lat_lng_m(center_lat)
            lat_len_m = lat_len_deg * lat_d_to_m
            lng_len_m = lng_len_deg * lng_d_to_m

            LOGGER.debug(
                "%s %s %s %s %s", center_lat, lat_len_deg,
                lng_len_deg, lat_len_m, lng_len_m)

            lat_m_to_deg = lat_len_deg / lat_len_m
            lng_m_to_deg = lng_len_deg / lng_len_m

            lat_extension_m = (500 - (lat_len_m/2))
            lng_extension_m = (500 - (lng_len_m/2))
            lat_extension_deg = lat_extension_m * lat_m_to_deg
            lng_extension_deg = lng_extension_m * lng_m_to_deg
            LOGGER.debug(
                '%s %s %s %s', lat_extension_deg, lng_extension_deg,
                lat_extension_m, lng_extension_m)
            bounding_box = [
                target_bounding_box[0]-lng_extension_deg,
                target_bounding_box[1]-lat_extension_deg,
                target_bounding_box[2]+lng_extension_deg,
                target_bounding_box[3]+lat_extension_deg,
            ]

            LOGGER.debug(
                "bounding box lengths %s %s",
                target_bounding_box[2]-target_bounding_box[0],
                target_bounding_box[3]-target_bounding_box[1])
            subprocess.run([
                'gdal_translate',
                '-projwin',
                str(target_bounding_box[0]),
                str(target_bounding_box[3]),
                str(target_bounding_box[2]),
                str(target_bounding_box[1]),
                '-of', 'PNG', granule_path, png_path],)
        if not os.path.exists(target_png_path):
            shutil.copy(png_path, target_png_path)


def get_dam_bounding_box_imagery_sentinel(
        task_graph, dam_id, image_bounding_box, workspace_dir):
    """Extract bounding box of grand sentinel imagery around point.

    Parameters:
        task_graph (TaskGraph): taskgraph for global scheduling.
        dam_id (str): unique identifier for dam
        image_bounding_box (list): [xmin, ymin, xmax, ymax] in WGS84 coords.
        workspace_dir (str): path to directory to write results into.

    Returns:
        List of local file path images.

    """
    LOGGER.debug(image_bounding_box)
    with sqlite3.connect(SENTINEL_SQLITE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT GRANULE_ID, CLOUD_COVER, SENSING_TIME, BASE_URL '
            'FROM sentinel_index WHERE '
            '  NORTH_LAT > ? AND '
            '  SOUTH_LAT < ? AND '
            '  WEST_LON < ? AND '
            '  EAST_LON > ? AND '
            '  CLOUD_COVER < 100.0 '
            'ORDER BY'
            '   CLOUD_COVER ASC,'
            '   SENSING_TIME DESC '
            'LIMIT 1;',
            (image_bounding_box[3], image_bounding_box[1],
             image_bounding_box[0], image_bounding_box[2]))

        for result in cursor:
            # make subidrectories so we don't get swamped with directories
            granule_dir = os.path.join(
                workspace_dir, 'granule_dir',
                result[0][-1], result[0][-2], result[0][-3], result[0][-4],
                result[0])
            try:
                os.makedirs(granule_dir)
            except OSError:
                pass

            gs_path = result[-1]
            bucket_id, subpath = (
                re.search('gs://([^/]+)/(.*)', gs_path).groups())

            url_prefix = f'''https://storage.googleapis.com/{
                bucket_id}/{subpath}'''

            manifest_url = f'{url_prefix}/manifest.safe'
            manifest_path = os.path.join(granule_dir, 'manifest.safe')

            if not os.path.exists(manifest_path):
                # download the manifest
                while True:
                    try:
                        r = requests.head(manifest_url, timeout=5)
                        break
                    except requests.Timeout:
                        LOGGER.exception('timed out, trying again')
                if r.status_code != requests.codes.ok:
                    return []
                urllib.request.urlretrieve(manifest_url, manifest_path)
            return fetch_tile_and_bound_data(
                task_graph, manifest_path,
                url_prefix, dam_id, image_bounding_box, granule_dir)


def fetch_tile_and_bound_data(
        task_graph, manifest_path, url_prefix,
        unique_id, bounding_box, target_dir):
    """Fetch tile from url defined in manifest_path.

    Parameters:
        task_graph (TaskGraph): taskgraph object to schedule against.
        manifest_path (str): path to a SAFE file describing sentinel data.
        url_prefix (str): url to prepend to any URLs needed by this function.
        unique_id (str): this string is appended to the fetched file
            to define the clipped version by the bounding box.
        bounding_box (list): [xmin, ymin, xmax, ymax]
        target_dir (str): path to directory which to download the data object.

    Returns:
        List of local png images.

    """
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    granule_path_list = []
    for band_id in (
            #'IMG_DATA_Band_TCI_Tile1_Data',
            'IMG_DATA_Band_10m_1_Tile1_Data',
            'IMG_DATA_Band_10m_2_Tile1_Data',
            'IMG_DATA_Band_10m_3_Tile1_Data',
            ):
        manifest_xml = etree.parse(manifest_path).getroot().xpath(
            f"//dataObject[@ID = '{band_id}']/*/fileLocation")
        if not manifest_xml:
            LOGGER.warn(f'{band_id} not found in {url_prefix}')
            continue
        raw_href = manifest_xml[0].get('href')
        # I think the API might think we're hacking a url ./
        clean_href = raw_href[2::] if raw_href.startswith('./') else raw_href
        granule_url = f"{url_prefix}/{clean_href}"
        granule_path = os.path.join(
            target_dir, os.path.basename(granule_url))
        if not os.path.exists(granule_path):
            try:
                urllib.request.urlretrieve(granule_url, granule_path)
            except:
                LOGGER.exception(f"couldn't get {granule_url}")
                return []
        granule_path_list.append(granule_path)

    granule_raster_info = pygeoprocessing.get_raster_info(
        granule_path_list[0])
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    target_bounding_box = pygeoprocessing.transform_bounding_box(
        bounding_box, wgs84_srs.ExportToWkt(),
        granule_raster_info['projection'], edge_samples=11)

    LOGGER.debug(
        "bounding box lengths %s %s",
        target_bounding_box[2]-target_bounding_box[0],
        target_bounding_box[3]-target_bounding_box[1])

    tif_path = f'{os.path.splitext(granule_path)[0]}_{unique_id}.tif'

    file_list_path = os.path.join(target_dir, f'{unique_id}_band_list.txt')
    if not os.path.exists(file_list_path):
        with open(file_list_path, 'w') as file_list_file:
            for granule_path in granule_path_list:
                file_list_file.write(f'{granule_path}\n')

    vrt_path = os.path.join(target_dir, f'{unique_id}_band_stack.vrt')
    if not os.path.exists(vrt_path):
        vrt_task = task_graph.add_task(
            func=subprocess.run,
            args=([
                'gdalbuildvrt', '-separate', '-input_file_list', file_list_path,
                vrt_path],),
            target_path_list=[vrt_path],
            task_name=f'build vrt {vrt_path}')

    if not os.path.exists(tif_path):
        warp_tif_task = task_graph.add_task(
            func=subprocess.run,
            args=([
                'gdal_translate',
                '-projwin',
                str(target_bounding_box[0]),
                str(target_bounding_box[3]),
                str(target_bounding_box[2]),
                str(target_bounding_box[1]),
                '-of', 'GTiff', vrt_path, tif_path],),
            target_path_list=[tif_path],
            dependent_task_list=[vrt_task],
            task_name=f'warp {tif_path}')
        warp_tif_task.join()

    tif_raster = gdal.OpenEx(tif_path, gdal.OF_RASTER)
    avg_val_list = []
    percentile_list = []
    for band_id in range(1, tif_raster.RasterCount+1):
        band = tif_raster.GetRasterBand(band_id)
        array = band.ReadAsArray()
        avg_val_list.append(numpy.median(array))
        percentile_list.append(
            numpy.percentile(array, [1, 99]))
        band = None
    tif_raster = None
    LOGGER.debug('percentile_list %s', percentile_list)
    avg_val = numpy.mean(avg_val_list)
    if avg_val < 500 or avg_val > 1800:
        # black image or too bright
        LOGGER.debug('black image or too bright %f', avg_val)
        return []

    png_path = f'{os.path.splitext(granule_path)[0]}_{unique_id}_{avg_val}.png'
    if not os.path.exists(png_path):
        warp_png_task = task_graph.add_task(
            func=subprocess.run,
            args=([
                'gdal_translate',
                '-of', 'PNG',
                '-scale_1',
                str(percentile_list[0][0]),
                str(percentile_list[0][1]), '0', str(65536),
                '-scale_2',
                str(percentile_list[1][0]),
                str(percentile_list[1][1]), '0', str(65536),
                '-scale_3',
                str(percentile_list[2][0]),
                str(percentile_list[2][1]), '0', str(65536),
                tif_path, png_path],),
            target_path_list=[png_path],
            dependent_task_list=[vrt_task],
            task_name=f'warp {png_path}')
        warp_png_task.join()
    return [png_path]


def extract_bounding_box(
        base_raster_path, bounding_box_tuple, bounding_box_ref_wkt,
        target_raster_path):
    """Extract a smaller box from the base.

    Parameters:
        base_raster_path (str): base raster
        bounding_box_tuple (tuple): bounding box to extract in lat/lng coords
            of the order [xmin, ymin, xmax, ymax].
        bounding_box_ref_wkt (str): spatial reference of bounding box in wkt.
        target_raster_path (str): created file that will be extracted from
            base_raster_path where `bounding_box_tuple` intersects with it.

    Returns:
        None

    """
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    target_bounding_box = pygeoprocessing.transform_bounding_box(
        bounding_box_tuple, bounding_box_ref_wkt,
        base_raster_info['projection'])
    pygeoprocessing.warp_raster(
        base_raster_path, base_raster_info['pixel_size'], target_raster_path,
        'near', target_bb=target_bounding_box)
    subprocess.run(
        ['gdal_translate', '-of', 'PNG', target_raster_path,
         f'{os.path.splitext(target_raster_path)[0]}.png'])


def gzip_csv_to_sqlite(base_gz_path, target_sqlite_path):
    """Convert gzipped csv to sqlite database."""
    if os.path.exists(target_sqlite_path):
        os.remove(target_sqlite_path)

    with gzip.open(base_gz_path, 'rt') as f_in:
        csv_reader = csv.reader(f_in)
        header_line = next(csv_reader)
        sample_first_line = next(csv_reader)

        column_text = ''.join([
            f'{column_id} {"FLOAT" if is_str_float(column_val) else "TEXT"} NOT NULL, '
            for column_id, column_val in zip(header_line, sample_first_line)])

        # we know a priori that column names are
        # 'GRANULE_ID', 'PRODUCT_ID', 'DATATAKE_IDENTIFIER', 'MGRS_TILE',
        # 'SENSING_TIME', 'TOTAL_SIZE', 'CLOUD_COVER',
        # 'GEOMETRIC_QUALITY_FLAG', 'GENERATION_TIME', 'NORTH_LAT',
        # 'SOUTH_LAT', 'WEST_LON', 'EAST_LON', 'BASE_URL'

        sql_create_index_table = (
            f"""
            CREATE TABLE sentinel_index (
                {column_text}
                PRIMARY KEY (GRANULE_ID)
            );
            CREATE UNIQUE INDEX NORTH_LAT_INDEX ON sentinel_index (NORTH_LAT);
            CREATE UNIQUE INDEX SOUTH_LAT_INDEX ON sentinel_index (SOUTH_LAT);
            CREATE UNIQUE INDEX WEST_LON_INDEX ON sentinel_index (WEST_LON);
            CREATE UNIQUE INDEX EAST_LON_INDEX ON sentinel_index (EAST_LON);
            """)

        LOGGER.debug(sql_create_index_table)

        LOGGER.info("building index")
        # reset to start and chomp the header row
        f_in.seek(0)
        header_line = next(csv_reader)

        line_count = 0
        n_bytes = 0
        last_time = time.time()
        with sqlite3.connect(target_sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_create_index_table)
            for line in csv_reader:
                cursor.execute(
                    f"""INSERT OR REPLACE INTO sentinel_index VALUES ({
                        ', '.join(['?']*len(header_line))})""", line)
                current_time = time.time()
                if current_time - last_time > REPORTING_INTERVAL:
                    last_time = current_time
                    LOGGER.info("inserted %d lines", line_count)
                line_count += 1
                n_bytes += len(''.join(line))


def is_str_float(val):
    """Return True if the string `val` can be a float."""
    try:
        _ = float(val)
        return True
    except ValueError:
        return False


def gunzip(base_path, target_path):
    """Gz-unzip `base_path` to `target_path`."""
    with gzip.open(base_path, 'rb') as f_in:
        with open(target_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def unzip(zipfile_path, target_dir, touchfile_path):
    """Unzip contents of `zipfile_path`.

    Parameters:
        zipfile_path (string): path to a zipped file.
        target_dir (string): path to extract zip file to.
        touchfile_path (string): path to a file to create if unzipping is
            successful.

    Returns:
        None.

    """
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    with open(touchfile_path, 'w') as touchfile:
        touchfile.write(f'unzipped {zipfile_path}')


def fetch_and_unzip(
        task_graph, bucket_id_path_tuple, iam_token_path, target_zipfile_dir):
    """Schedule a bucket fetch and file unzip.

    Parameters:
        task_graph (TaskGraph): taskgraph object to schedule download and
            unzip.
        bucket_id_path_tuple (tuple): a google bucket id / path tuple for the
            expected bucket and the path inside it to the zipfile.
        iam_token_path (str): path to IAM token to access `bucket_id`.
        target_zipfile_dir (str): desired directory to copy `bucket_path` and
            unzip its contents. This directory will also have an
            `os.path.basename(bucket_id_path_tuple[1]).UNZIPPED` file when the
            unzip task is successful.

    Returns:
        Task object that when complete will have downloaded and unzipped
            the files specified above.

    """
    zipfile_basename = os.path.basename(bucket_id_path_tuple[1])
    expected_zip_path = os.path.join(target_zipfile_dir, zipfile_basename)
    fetch_task = task_graph.add_task(
        func=reproduce.utils.google_bucket_fetch_and_validate,
        args=(
            bucket_id_path_tuple[0], bucket_id_path_tuple[1], iam_token_path,
            expected_zip_path),
        target_path_list=[expected_zip_path],
        task_name=f'''fetch {zipfile_basename}''')

    unzip_file_token = f'{expected_zip_path}.UNZIPPED'
    unzip_task = task_graph.add_task(
        func=unzip,
        args=(
            expected_zip_path, target_zipfile_dir, unzip_file_token),
        target_path_list=[unzip_file_token],
        dependent_task_list=[fetch_task],
        task_name=f'unzip {zipfile_basename}')
    return unzip_task


def monitor_validation_database(validation_database_path):
    """Continuously monitor the validation database.

    Parameters:
        validation_database_path (str): database to watch for unfetched
            bounding boxes, specifically the `validation_table` table.

    Returns:
        None.

    """
    try:
        os.makedirs(DAM_IMAGERY_DIR)
    except OSError:
        pass

    """
    planet_asset_fetch_queue = queue.Queue(100)
    planet_imagery_dir = os.path.join(DAM_IMAGERY_DIR, 'planet')
    planet_workspace_dir = os.path.join(WORKSPACE_DIR, 'planet')
    process_planet_asset_fetch_queue_thread = threading.Thread(
        target=process_planet_asset_fetch_queue,
        args=(
            planet_asset_fetch_queue, planet_imagery_dir,
            planet_workspace_dir))
    process_planet_asset_fetch_queue_thread.start()
    """
    largest_bounding_box_id = -1
    bounding_box_db_conn = sqlite3.connect(BOUNDING_BOX_DB_PATH)
    cursor = bounding_box_db_conn.execute(
        'SELECT bounding_box_id from bounding_box_imagery_table '
        'ORDER BY bounding_box_id DESC LIMIT 1;')
    payload = cursor.fetchone()
    if payload is not None:
        largest_bounding_box_id = int(payload[0])

    sentinel_imagery_dir = os.path.join(DAM_IMAGERY_DIR, 'sentinel')
    try:
        os.makedirs(sentinel_imagery_dir)
    except:
        pass
    sentinel_workspace_dir = os.path.join(WORKSPACE_DIR, 'sentinel')
    last_time = time.time()
    largest_validation_id = -1
    cursor = bounding_box_db_conn.execute(
        'SELECT validation_id from bounding_box_imagery_table '
        'ORDER BY validation_id DESC LIMIT 1;')
    payload = cursor.fetchone()
    if payload is not None:
        largest_validation_id = int(payload[0])
    val_db_conn = sqlite3.connect(validation_database_path)
    while True:
        current_time = time.time()
        if current_time-last_time < POLL_RATE:
            time.sleep(POLL_RATE-(current_time-last_time))
        last_time = time.time()
        LOGGER.info(
            'trying new select with largest_validation_id=%d '
            'largest_bounding_box_id=%d',
            largest_validation_id, largest_bounding_box_id)
        for payload in val_db_conn.execute(
                'SELECT bounding_box_bounds, metadata, id, '
                'base_table.database_id, base_table.source_key '
                'FROM validation_table '
                'INNER JOIN base_table on '
                'validation_table.key = base_table.key '
                'WHERE id > ? '
                'ORDER BY id ASC;', (largest_validation_id,)):
            (dam_bb_bounds_json, metadata, validation_id,
             database_id, source_key) = payload

            unique_id = f'{database_id}-{source_key}'
            LOGGER.info('processing %d', validation_id)
            largest_validation_id = max(
                largest_validation_id, validation_id)

            # fetch imagery list that intersects with the bounding box
            # [minx,miny,maxx,maxy]
            if dam_bb_bounds_json == 'None':
                LOGGER.info('no bounding box for %s', unique_id)
                continue
            dam_bb_bounds = json.loads(dam_bb_bounds_json.replace("'", '"'))
            if dam_bb_bounds is not None:
                LOGGER.debug(dam_bb_bounds[0])

                lat_len_deg = abs(
                    dam_bb_bounds[0]['lat']-dam_bb_bounds[1]['lat'])
                lng_len_deg = abs(
                    dam_bb_bounds[0]['lng']-dam_bb_bounds[1]['lng'])
                center_lat = (
                    dam_bb_bounds[0]['lat']+dam_bb_bounds[1]['lat']) / 2

                lat_d_to_m, lng_d_to_m = len_of_deg_to_lat_lng_m(center_lat)
                lat_len_m = lat_len_deg * lat_d_to_m
                lng_len_m = lng_len_deg * lng_d_to_m

                LOGGER.debug(
                    "%s %s %s %s %s", center_lat, lat_len_deg,
                    lng_len_deg, lat_len_m, lng_len_m)

                if lat_len_m == 0 or lng_len_m == 0:
                    LOGGER.debug("got some zero length bounding_box, skipping")
                    continue
                lat_m_to_deg = lat_len_deg / lat_len_m
                lng_m_to_deg = lng_len_deg / lng_len_m

                lat_extension_m = (500 - (lat_len_m/2))
                lng_extension_m = (500 - (lng_len_m/2))
                lat_extension_deg = lat_extension_m * lat_m_to_deg
                lng_extension_deg = lng_extension_m * lng_m_to_deg
                LOGGER.debug(
                    '%s %s %s %s', lat_extension_deg, lng_extension_deg,
                    lat_extension_m, lng_extension_m)
                image_bounding_box = [
                    min(dam_bb_bounds[0]['lng'], dam_bb_bounds[1]['lng'])-lng_extension_deg,
                    min(dam_bb_bounds[0]['lat'], dam_bb_bounds[1]['lat'])-lat_extension_deg,
                    max(dam_bb_bounds[0]['lng'], dam_bb_bounds[1]['lng'])+lng_extension_deg,
                    max(dam_bb_bounds[0]['lat'], dam_bb_bounds[1]['lat'])+lat_extension_deg,
                ]
                image_north_lat = image_bounding_box[3]
                image_south_lat = image_bounding_box[1]
                image_west_lon = image_bounding_box[2]
                image_east_lon = image_bounding_box[0]
                """
                get_dam_bounding_box_imagery_planet(
                    task_graph, unique_id, image_bounding_box,
                    planet_workspace_dir,
                    planet_asset_fetch_queue)
                time.sleep(0.4)
                """
                imagery_path_list = get_dam_bounding_box_imagery_sentinel(
                    task_graph, unique_id, image_bounding_box,
                    sentinel_workspace_dir)
                if imagery_path_list:
                    for imagery_path in imagery_path_list:
                        LOGGER.info(
                            'copying %s to %s', imagery_path,
                            sentinel_imagery_dir)
                        shutil.copy(imagery_path, sentinel_imagery_dir)
                        dam_north_lat = max(
                            dam_bb_bounds[0]['lat'], dam_bb_bounds[1]['lat'])
                        dam_south_lat = min(
                            dam_bb_bounds[0]['lat'], dam_bb_bounds[1]['lat'])
                        dam_west_lng = min(
                            dam_bb_bounds[0]['lng'], dam_bb_bounds[1]['lng'])
                        dam_east_lng = max(
                            dam_bb_bounds[0]['lng'], dam_bb_bounds[1]['lng'])

                        # this is the primary key for the bounding boxes
                        largest_bounding_box_id += 1
                        bounding_box_db_conn.execute(
                            'INSERT INTO bounding_box_imagery_table '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            (validation_id, image_north_lat, image_south_lat,
                             image_west_lon, image_east_lon, dam_north_lat,
                             dam_south_lat, dam_west_lng, dam_east_lng,
                             imagery_path, largest_bounding_box_id))
                        bounding_box_db_conn.commit()
                else:
                    LOGGER.warn(
                        'no valid imagery found for %s', unique_id)

                not_a_dam_index = 0
                while True:
                    not_a_dam_id = f'{unique_id}_nodam_{not_a_dam_index}'
                    lat_int = int(image_north_lat)
                    lng_int = int(image_west_lon)
                    surface_water_tif_url = (
                        f'''http://storage.googleapis.com/'''
                        f'''global-surface-water/downloads/occurrence/'''
                        f'''occurrence_{
                            abs(floor_down(lng_int))}{
                            'E' if lng_int > 0 else 'W'}_{
                            abs(floor_down(lat_int))}{
                            'N' if lat_int > 0 else 'S'}.tif''')
                    LOGGER.debug("going to fetch %s", surface_water_tif_url)
                    surface_water_tif_path = os.path.join(
                        WORKSPACE_DIR, os.path.basename(
                            surface_water_tif_url))
                    if not os.path.exists(surface_water_tif_path):
                        LOGGER.info(
                            'downloading %s' % surface_water_tif_url)
                        urllib.request.urlretrieve(
                            surface_water_tif_url, surface_water_tif_path)

                    raster = gdal.OpenEx(surface_water_tif_path, gdal.OF_RASTER)
                    band = raster.GetRasterBand(1)
                    n_pixels = min(band.XSize, band.YSize)-SEARCH_BLOCK_SIZE
                    LOGGER.debug('searching blocks of size %s in the range %s' % (
                        SEARCH_BLOCK_SIZE, str((n_pixels, n_pixels))))
                    while True:
                        # pick a point that's within range on the base tile
                        sample_point = numpy.random.randint(
                            SEARCH_BLOCK_SIZE, n_pixels, (2,), dtype=numpy.int32)
                        # pull a block around that point
                        sample_block = band.ReadAsArray(
                            xoff=float(sample_point[0]),
                            yoff=float(sample_point[1]),
                            win_xsize=SEARCH_BLOCK_SIZE,
                            win_ysize=SEARCH_BLOCK_SIZE)
                        # search for pixels there that include edge surface water
                        partial_samples = numpy.argwhere(
                            (sample_block > MIN_SURFACE_WATER) &
                            (sample_block < MAX_SURFACE_WATER))
                        # if we found any, break
                        if partial_samples.size > 0:
                            break
                    random_sample_block_coord = partial_samples[
                        numpy.random.randint(0, partial_samples.shape[0], 1)]
                    # convert back to raster global coordinate
                    global_coordinate = (
                        sample_point[1]+random_sample_block_coord[0][0],
                        sample_point[0]+random_sample_block_coord[0][1])
                    gt = raster.GetGeoTransform()
                    # convert back to lat/lng
                    lng_coord = gt[0]+global_coordinate[1]*gt[1]
                    lat_coord = gt[3]+global_coordinate[0]*gt[5]

                    lat_d_to_m, lng_d_to_m = len_of_deg_to_lat_lng_m(
                        lat_coord)
                    lat_extension_deg = 500 / lat_d_to_m
                    lng_extension_deg = 500 / lng_d_to_m
                    LOGGER.debug(
                        '%s %s %s %s', lng_coord, lat_coord, lat_extension_deg, lng_extension_deg)
                    image_bounding_box = [
                        lng_coord-lng_extension_deg,
                        lat_coord-lat_extension_deg,
                        lng_coord+lng_extension_deg,
                        lat_coord+lat_extension_deg,
                    ]
                    """
                    get_dam_bounding_box_imagery_planet(
                        task_graph, not_a_dam_id, image_bounding_box,
                        planet_workspace_dir,
                        planet_asset_fetch_queue)
                    time.sleep(0.4)
                    """
                    imagery_path_list = get_dam_bounding_box_imagery_sentinel(
                        task_graph, not_a_dam_id, image_bounding_box,
                        sentinel_workspace_dir)
                    LOGGER.debug(
                        "searched %s got %s", image_bounding_box,
                        imagery_path_list)
                    if imagery_path_list:
                        for imagery_path in imagery_path_list:
                            LOGGER.info(
                                'copying %s to %s', imagery_path,
                                sentinel_imagery_dir)
                            shutil.copy(imagery_path, sentinel_imagery_dir)
                            image_north_lat = image_bounding_box[3]
                            image_south_lat = image_bounding_box[1]
                            image_west_lon = image_bounding_box[2]
                            image_east_lon = image_bounding_box[0]
                            dam_north_lat = max(
                                dam_bb_bounds[0]['lat'],
                                dam_bb_bounds[1]['lat'])
                            dam_south_lat = min(
                                dam_bb_bounds[0]['lat'],
                                dam_bb_bounds[1]['lat'])
                            dam_west_lng = min(
                                dam_bb_bounds[0]['lng'],
                                dam_bb_bounds[1]['lng'])
                            dam_east_lng = max(
                                dam_bb_bounds[0]['lng'],
                                dam_bb_bounds[1]['lng'])

                            # this is the primary key for the bounding boxes
                            largest_bounding_box_id += 1
                            bounding_box_db_conn.execute(
                                'INSERT INTO bounding_box_imagery_table '
                                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                                (validation_id, image_north_lat,
                                 image_south_lat, image_west_lon,
                                 image_east_lon, None, None, None, None,
                                 imagery_path,
                                 largest_bounding_box_id))
                            bounding_box_db_conn.commit()
                    else:
                        LOGGER.warn(
                            'no valid imagery found for %s', not_a_dam_id)
                    break
                else:
                    LOGGER.info('no bounding box registered')
                # test if it's valid (no black?)
                # if not, save it and record in database?
    val_db_conn.close()


def len_of_deg_to_lat_lng_m(center_lat):
    """Calculate length of degree in m.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Parameters:
        len_in_deg (float): length in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.

    Returns:
        Area of square pixel of side length `pixel_size` centered at
        `center_lat` in m^2.

    """
    # Convert latitude to radians
    lat = center_lat * math.pi / 180
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118

    latlen = (
        m1 + m2*math.cos(2*lat) + m3*math.cos(4*lat) + m4*math.cos(6*lat))
    longlen = (
        p1*math.cos(lat) + p2*math.cos(3*lat) + p3*math.cos(5*lat))
    return (latlen, longlen)


def build_database(database_path):
    """Create the bounding box table."""
    sql_create_table_command = (
        """
        CREATE TABLE IF NOT EXISTS bounding_box_imagery_table (
            validation_id INTEGER NOT NULL,
            image_north_lat FLOAT NOT NULL,
            image_south_lat FLOAT NOT NULL,
            image_west_lon FLOAT NOT NULL,
            image_east_lon FLOAT NOT NULL,
            dam_north_lat FLOAT,
            dam_south_lat FLOAT,
            dam_west_lng FLOAT,
            dam_east_lng FLOAT,
            image_filename TEXT NOT NULL,
            bounding_box_id INTEGER NOT NULL PRIMARY KEY
        );
        CREATE UNIQUE INDEX IF NOT EXISTS validation_key
        ON bounding_box_imagery_table (validation_id);
        CREATE UNIQUE INDEX IF NOT EXISTS bounding_box_imagery_key
        ON bounding_box_imagery_table (bounding_box_id);
        """)
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.executescript(sql_create_table_command)


def floor_down(val):
    """Floor down to the nearest 10. 123 to 120, -119 to -120."""
    if val > 0:
        result = math.ceil(val / 10) * 10
    else:
        result = math.floor(val / 10) * 10
    return int(numpy.sign(val) * result)


if __name__ == '__main__':
    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, -1,
        reporting_interval=REPORTING_INTERVAL)
    build_index(task_graph)
    build_database(BOUNDING_BOX_DB_PATH)
    monitor_validation_database(data_validator.DATABASE_PATH)
