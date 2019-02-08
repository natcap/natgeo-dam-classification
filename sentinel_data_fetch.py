"""Create a sentinel sqlite database index."""
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

import urllib.request
import urllib.error
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import shapely.wkb
import reproduce
import pygeoprocessing

OPENER = urllib.request.build_opener()
OPENER.addheaders = [('User-agent', 'Mozilla/5.0')]
urllib.request.install_opener(OPENER)

WORKSPACE_DIR = 'workspace'
SENTINEL_CSV_BUCKET_ID_TUPLE = ('gcp-public-data-sentinel-2', 'index.csv.gz')
SENTINEL_CSV_GZ_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.csv.gz')
SENTINEL_SQLITE_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.db')
GRAND_VECTOR_BUCKET_ID_TUPLE = (
    'natgeo-data-bucket',
    'GRanD_Version_1_1_md5_9ad04293d056cd35abceb8a15b953fb8.zip')
GRAND_VECTOR_PATH = os.path.join(
    WORKSPACE_DIR, 'GRanD_Version_1_1', 'GRanD_dams_v1_1.shp')
COVERAGE_VECTOR_PATH = os.path.join(WORKSPACE_DIR, 'grand_coverage.gpkg')
IAM_TOKEN_PATH = 'ecoshard-202992-key.json'

LOGGER = logging.getLogger(__name__)

BB_RADIUS = 500


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

    _ = fetch_and_unzip(
        task_graph, GRAND_VECTOR_BUCKET_ID_TUPLE, IAM_TOKEN_PATH,
        WORKSPACE_DIR)

    task_graph.join()


def get_bounding_box_imagery(
        task_graph, sample_point, point_id, workspace_dir,
        fetch_if_not_downloaded=True):
    """Extract bounding box of grand sentinel imagery around point.

    Parameters:
        task_graph (TaskGraph): taskgraph for global scheduling.
        sample_point (shapely.Point): sample point to center bounding box.
        point_id (string): string to uniquely identify the point for file
            naming schemes.
        workspace_dir (str): path to directory to write results into.
        fetch_if_not_downloaded (bool): if False, raises a StopIteration if
            the necessary tile has not been fetched. Otherwise downloads tile.

    Returns:
        Local file path location of image.

    """
    bounding_box = sample_point.buffer(BB_RADIUS/110000.0).bounds
    with sqlite3.connect(SENTINEL_SQLITE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT GRANULE_ID, CLOUD_COVER, SENSING_TIME, BASE_URL '
            'FROM sentinel_index WHERE '
            '  NORTH_LAT > ? AND '
            '  SOUTH_LAT < ? AND '
            '  WEST_LON < ? AND '
            '  EAST_LON > ?'
            'ORDER BY'
            '   CLOUD_COVER ASC,'
            '   SENSING_TIME DESC '
            'LIMIT 1;',
            (bounding_box[3], bounding_box[1],
             bounding_box[0], bounding_box[2]))

        for result in cursor:
            granule_dir = os.path.join(workspace_dir, result[0])
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
            if fetch_if_not_downloaded and not os.path.exists(manifest_path):
                raise StopIteration('manifest not downloaded')
            manifest_task_fetch = task_graph.add_task(
                func=urllib.request.urlretrieve,
                args=(manifest_url, manifest_path),
                target_path_list=[manifest_path],
                task_name=f'fetch {manifest_url}')

            return fetch_tile_and_bound_data(
                task_graph, [manifest_task_fetch], manifest_path,
                url_prefix, point_id, sample_point, granule_dir)


def fetch_tile_and_bound_data(
        task_graph, dependent_task_list, manifest_path, url_prefix,
        unique_id, sample_point, target_dir):
    """Fetch tile from url defined in manifest_path.

    Parameters:
        task_graph (TaskGraph): taskgraph object to schedule against.
        dependent_task_list (list): list of tasks that need to be executed
            first before any scheduling.
        manifest_path (str): path to a SAFE file describing sentinel data.
        url_prefix (str): url to prepend to any URLs needed by this function.
        unique_id (str): this string is appended to the fetched file
            to define the clipped version by the bounding box.
        sample_point (shapely.Point): point to center bounding box on in
            WGS84 coordinate system.
        target_dir (str): path to directory which to download the data object.

    Returns:
        Path to local png image.

    """
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    for band_id in (
            'IMG_DATA_Band_TCI_Tile1_Data',
            #'IMG_DATA_Band_10m_1_Tile1_Data',
            #'IMG_DATA_Band_10m_2_Tile1_Data',
            #'IMG_DATA_Band_10m_3_Tile1_Data',
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
        fetch_attempt = 0
        while True:
            try:
                fetch_attempt += 1
                fetch_task = task_graph.add_task(
                    func=urllib.request.urlretrieve,
                    args=(granule_url, granule_path),
                    target_path_list=[granule_path],
                    dependent_task_list=dependent_task_list,
                    task_name=f'fetch {granule_path}')
                fetch_task.join()
                break
            except:
                if fetch_attempt == 5:
                    raise
                sleep_time = fetch_attempt * 5.0
                LOGGER.exception(
                    f"couldn't get {granule_url}, waiting {sleep_time}s")
                time.sleep(sleep_time)

        granule_raster_info = pygeoprocessing.get_raster_info(granule_path)

        granule_srs = osr.SpatialReference()
        granule_srs.ImportFromWkt(granule_raster_info['projection'])
        wgs84_to_granule = osr.CoordinateTransformation(
            wgs84_srs, granule_srs)
        point = ogr.CreateGeometryFromWkt(sample_point.wkt)
        point.Transform(wgs84_to_granule)
        granule_point = shapely.wkt.loads(point.ExportToWkt())
        target_bounding_box = granule_point.buffer(BB_RADIUS).bounds

        # this will be a GeoTIFF, hence the .tif suffix
        clipped_raster_path = (
            f'{os.path.splitext(granule_path)[0]}_{unique_id}.tif')
        warp_task = task_graph.add_task(
            func=pygeoprocessing.warp_raster,
            args=(
                granule_path, granule_raster_info['pixel_size'],
                clipped_raster_path, 'near'),
            kwargs={'target_bb': target_bounding_box},
            target_path_list=[clipped_raster_path],
            dependent_task_list=[fetch_task],
            task_name=f'clip {clipped_raster_path}')
        png_path = f'{os.path.splitext(clipped_raster_path)[0]}.png'
        warp_task = task_graph.add_task(
            func=subprocess.run,
            args=([
                'gdal_translate', '-of', 'PNG', clipped_raster_path,
                png_path],),
            target_path_list=[png_path],
            dependent_task_list=[warp_task],
            task_name=f'warp {png_path}')
        warp_task.join()
        return png_path


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
