"""Create a sentinel sqlite database index."""
import itertools
import csv
import gzip
import shutil
import re
import sys
import os
import logging
import sqlite3

import google.cloud.storage
import taskgraph

WORKSPACE_DIR = 'workspace'
SENTINEL_CSV_INDEX_GS = 'gs://gcp-public-data-sentinel-2/index.csv.gz'
SENTINEL_CSV_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.csv.gz')
SENTINEL_SQLITE_PATH = os.path.join(WORKSPACE_DIR, 'sentinel_index.db')
IAM_TOKEN_PATH = 'ecoshard-202992-key.json'

logging.getLogger('taskgraph').setLevel(logging.INFO)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Entry point."""
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1, 5.0)

    download_index_task = task_graph.add_task(
        func=google_bucket_fetcher(
            SENTINEL_CSV_INDEX_GS, IAM_TOKEN_PATH),
        args=(SENTINEL_CSV_PATH,),
        target_path_list=[SENTINEL_CSV_PATH],
        task_name=f'fetch sentinel index')

    task_graph.add_task(
        func=gzip_csv_to_sqlite,
        args=(SENTINEL_CSV_PATH, SENTINEL_SQLITE_PATH),
        target_path_list=[SENTINEL_SQLITE_PATH],
        dependent_task_list=[download_index_task],
        task_name='unzip index csv')

    task_graph.close()
    task_graph.join()


def gzip_csv_to_sqlite(base_gz_path, target_sqlite_path):
    """Convert gzipped csv to sqlite database."""
    if os.path.exists(target_sqlite_path):
        os.remove(target_sqlite_path)

    with gzip.open(base_gz_path, 'rt') as f_in:
        csv_reader = csv.reader(f_in)
        header_line = next(csv_reader)
        sample_first_line = next(csv_reader)

        # put sample first line back
        csv_reader = itertools.chain([sample_first_line], csv_reader)

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

        with sqlite3.connect(target_sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_create_index_table)
            for line in csv_reader:
                cursor.execute(
                    f"""INSERT OR REPLACE INTO sentinel_index VALUES ({
                        ', '.join(['?']*len(header_line))})""", line)


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


def google_bucket_fetcher(url, json_key_path):
    """Closure for a function to download a Google Blob to a given path.

    Parameters:
        url (string): url to blob, matches the form
            '^https://storage.cloud.google.com/([^/]*)/(.*)$'
        json_key_path (string): path to Google Cloud private key generated
            by https://cloud.google.com/iam/docs/creating-managing-service-account-keys

    Returns:
        a function with a single `path` argument to the target file. Invoking
            this function will download the Blob to `path`.

    """
    def _google_bucket_fetcher(path):
        """Fetch blob `url` to `path`."""
        if url.startswith('https'):
            url_matcher = re.match(r'^https://[^/]*\.com/([^/]*)/(.*)$', url)
        elif url.startswith('gs'):
            url_matcher = re.match(r'^gs://([^/]*)/(.*)$', url)

        LOGGER.debug(url)
        client = google.cloud.storage.client.Client.from_service_account_json(
            json_key_path)
        bucket_id = url_matcher.group(1)
        LOGGER.debug(f'parsing bucket {bucket_id} from {url}')
        bucket = client.get_bucket(bucket_id)
        blob_id = url_matcher.group(2)
        LOGGER.debug(f'loading blob {blob_id} from {url}')
        blob = google.cloud.storage.Blob(blob_id, bucket)
        LOGGER.info(f'downloading blob {path} from {url}')
        blob.download_to_filename(path)
    return _google_bucket_fetcher

if __name__ == '__main__':
    main()
