"""Create a sentinel sqlite database index."""
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

import google.cloud.storage
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import shapely.wkb
import reproduce
import taskgraph

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

#logging.getLogger('taskgraph').setLevel(logging.INFO)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
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
    client = google.cloud.storage.client.Client.from_service_account_json(
        iam_token_path)
    grand_vector = gdal.OpenEx(grand_vector_path, gdal.OF_VECTOR)
    grand_layer = grand_vector.GetLayer()
    for grand_point_feature in grand_layer:
        grand_point_geom = grand_point_feature.GetGeometryRef()
        grand_point_shapely = shapely.wkb.loads(
            grand_point_geom.ExportToWkb())

        # 300m in degrees at equator
        grand_bb = grand_point_shapely.buffer(0.002713115198871344).bounds

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
                gs_path = result[-1]
                bucket_id, subpath = (
                    re.search('gs://([^/]+)/(.*)', gs_path).groups())
                LOGGER.debug('%s %s', bucket_id, subpath)
                bucket = client.get_bucket(bucket_id)
                blob_list = [
                    blob for blob in bucket.list_blobs(prefix=subpath)
                    if blob.name.endswith('TCI.jp2')]

                granule_dir = os.path.join(workspace_dir, result[0])
                try:
                    os.makedirs(granule_dir)
                except OSError:
                    pass

                for blob in blob_list:
                    local_blob_path = os.path.join(
                        granule_dir, os.path.basename(blob.name))

                    download_blob_task = task_graph.add_task(
                        func=reproduce.utils.google_bucket_fetch,
                        args=(
                            bucket_id, blob.name,
                            IAM_TOKEN_PATH, local_blob_path),
                        target_path_list=[local_blob_path],
                        task_name=f'fetch {local_blob_path}')
        break

        """
        # create a bounding box polygon and dump to vector so we can examine
        # it while the downloads process
        coverage_vector = gdal.OpenEx(
            COVERAGE_VECTOR_PATH, gdal.OF_VECTOR | gdal.GA_Update)
        coverage_layer = coverage_vector.GetLayer()
        coverage_defn = coverage_layer.GetLayerDefn()
        grand_bb_feature = ogr.Feature(coverage_defn)
        grand_bb_feature.SetGeometry(ogr.CreateGeometryFromWkt(grand_bb.wkt))
        grand_bb_feature.SetField('coverage_or_boundingbox', 'boundingbox')
        grand_bb_feature.SetField(
            'grand_id', grand_point_feature.GetField('GRAND_ID'))
        coverage_layer.CreateFeature(grand_bb_feature)
        coverage_layer.SyncToDisk()
        coverage_layer = None
        coverage_vector = None
        grand_bb_feature = None
        """



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
