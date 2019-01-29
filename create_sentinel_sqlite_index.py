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
from osgeo import gdal
from osgeo import osr
import shapely.wkb
import reproduce
import taskgraph
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

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
logging.getLogger('taskgraph').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('google').setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)
N_WORKERS = -1
REPORTING_INTERVAL = 5.0


def main():
    """Entry point."""
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass

    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)

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

    granules_dir = os.path.join(WORKSPACE_DIR, 'sentinel_granules')
    schedule_grand_sentinel_extraction(
        task_graph, GRAND_VECTOR_PATH, SENTINEL_SQLITE_PATH, IAM_TOKEN_PATH,
        granules_dir)

    task_graph.close()
    task_graph.join()


def schedule_grand_sentinel_extraction(
        task_graph, grand_vector_path, sentinel_sqlite_index_path,
        iam_token_path, workspace_dir):
    """Extract bounding boxes of grand sentinel imagery around points.

    Parameters:
        task_graph (TaskGraph): TaskGraph object to schedule on
        grand_vector_path (str): path to GRaND point database.
        sentinel_sqlite_index_path (str): path to Sqlite sentinel index for
            google's buckets. Contains a table called ' sentinel_index' with
            these columns:
                'GRANULE_ID', 'PRODUCT_ID', 'DATATAKE_IDENTIFIER',
                'MGRS_TILE', 'SENSING_TIME', 'TOTAL_SIZE', 'CLOUD_COVER',
                'GEOMETRIC_QUALITY_FLAG', 'GENERATION_TIME', 'NORTH_LAT',
                'SOUTH_LAT', 'WEST_LON', 'EAST_LON', 'BASE_URL'
        iam_token_path (str): path to a google IAM json file that was
            generated by https://cloud.google.com/iam/docs/creating-managing-service-account-keys
            that has access to the sentinel google buckets.
        workspace_dir (str): path to directory to write results into.

    Returns:
        Task object that can be joined when all scheduled tasks are complete.

    """
    grand_vector = gdal.OpenEx(grand_vector_path, gdal.OF_VECTOR)
    grand_layer = grand_vector.GetLayer()

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    wgs84_srs_wkt = wgs84_srs.ExportToWkt()

    for grand_point_feature in grand_layer:
        grand_id = grand_point_feature.GetField('GRAND_ID')
        grand_point_geom = grand_point_feature.GetGeometryRef()
        grand_point_shapely = shapely.wkb.loads(
            grand_point_geom.ExportToWkb())

        # 2000m bb at equator
        grand_bb = grand_point_shapely.buffer(2000/110000.0).bounds

        with sqlite3.connect(sentinel_sqlite_index_path) as conn:
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
                '   SENSING_TIME DESC;',
                (grand_bb[3], grand_bb[1], grand_bb[0], grand_bb[2]))

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
                manifest_task_fetch = task_graph.add_task(
                    func=urllib.request.urlretrieve,
                    args=(manifest_url, manifest_path),
                    target_path_list=[manifest_path],
                    task_name=f'fetch {manifest_url}')

                for data_tag in ('IMG_DATA_Band_TCI_Tile1_Data',):
                    complete_token_path = os.path.join(
                        granule_dir, f'{data_tag}.COMPLETE')
                    granule_task_fetch = task_graph.add_task(
                        func=fetch_tile_and_bound_data,
                        args=(
                            manifest_path, url_prefix, data_tag,
                            'grand_%d' % grand_id, grand_bb, wgs84_srs_wkt,
                            granule_dir, complete_token_path),
                        dependent_task_list=[manifest_task_fetch],
                        target_path_list=[complete_token_path],
                        task_name=f'fetch {data_tag}')


def fetch_tile_and_bound_data(
        manifest_path, url_prefix, data_tag, bound_id_suffix, bounding_box,
        bounding_box_ref_wkt, target_dir, complete_token_path):
    """Fetch tile from url defined in manifest_path.

    Parameters:
        manifest_path (str): path to a SAFE file describing sentinel data.
        url_prefix (str): url to prepend to any URLs needed by this function.
        data_tag (str): embedded dataObject tag to fetch.
        bound_id_suffix (str): this string is appended to the fetched file
            to define the clipped version by the bounding box.
        bounding_box (tuple): 4 tuple of the form [xmin, ymin, xmax, ymax].
        bounding_box_ref_wkt (str): wkt coordinate reference system for
            `bounding_box`.
        target_dir (str): path to directory which to download the data object.
        complete_token_path (str): path to a file to touch when fetch is
            complete. This helps avoid repeated reexecution when target path
            is otherwise unknown.

    Returns:
        None

    """
    for band_id in (
            'IMG_DATA_Band_TCI_Tile1_Data',
            'IMG_DATA_Band_10m_1_Tile1_Data',
            'IMG_DATA_Band_10m_2_Tile1_Data',
            'IMG_DATA_Band_10m_3_Tile1_Data'):
        manifest_xml = etree.parse(manifest_path).getroot().xpath(
            f"//dataObject[@ID = '{band_id}']/*/fileLocation")
        if not manifest_xml:
            LOGGER.warn(f'{band_id} not found in {url_prefix}')
            continue
        granule_url = f"{url_prefix}/{manifest_xml[0].get('href')}"
        granule_path = os.path.join(
            target_dir, os.path.basename(granule_url))
        try:
            urllib.request.urlretrieve(granule_url, granule_path)
        except:
            LOGGER.exception(f"couldn't get {granule_url}")

        granule_raster_info = pygeoprocessing.get_raster_info(granule_path)
        target_bounding_box = pygeoprocessing.transform_bounding_box(
            bounding_box, bounding_box_ref_wkt,
            granule_raster_info['projection'])
        # this will be a GeoTIFF, hence the .tif suffix
        clipped_raster_path = (
            f'{os.path.splitext(granule_path)[0]}_{bound_id_suffix}.tif')
        pygeoprocessing.warp_raster(
            granule_path, granule_raster_info['pixel_size'],
            clipped_raster_path, 'near', target_bb=target_bounding_box)
        subprocess.run(
            ['gdal_translate', '-of', 'PNG', clipped_raster_path,
             f'{os.path.splitext(clipped_raster_path)[0]}.png'])

        with open(complete_token_path, 'w') as complete_token_file:
            complete_token_file.write(granule_url)


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


if __name__ == '__main__':
    main()
