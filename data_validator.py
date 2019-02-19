"""Flask app to validata imagery and point locations."""
import io
import zipfile
import json
import datetime
import sqlite3
import os
import sys
import logging
import threading
import time

import reproduce.utils
import taskgraph
import shapely.wkt
import shapely.geometry
from osgeo import gdal
from flask import Flask
import flask


LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

APP = Flask(__name__, static_url_path='', static_folder='')
VISITED_POINT_ID_TIMESTAMP_MAP = {}
WORKSPACE_DIR = 'workspace'

GRAND_VERSION_1_1 = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/GRanD_Version_1_1_md5_9ad04293d056cd35abceb8a15b953fb8.zip'
POINT_DAM_DATA_MAP = {
    'GRAND': {
        'database_url': GRAND_VERSION_1_1,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, 'GRanD_Version_1_1/GRanD_dams_v1_1.shp'),
        'database_key': 'GRAND_ID',
    }
}

VALIDATION_DATABASE_PATH = os.path.join(WORKSPACE_DIR)
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'dam_bounding_box_db.db')
N_WORKERS = -1
REPORTING_INTERVAL = 5.0
DEFAULT_COMMENT_BOX_TEXT = '(optional comments)'
ACTIVE_DELAY = 30.0  # wait this many seconds before trying point again

@APP.route('/')
def entry_point():
    """Root GET."""
    return process_point('0')

@APP.route('/favicon.ico')
def favicon():
    return flask.send_from_directory(
        os.path.join(APP.root_path, 'images'), 'favicon.ico',
        mimetype='image/vnd.microsoft.icon')

@APP.route('/unvalidated')
def get_unvalidated_point():
    """Get a point that has not been validated."""
    LOGGER.debug('trying to get an unvalidated point')
    try:
        with DB_LOCK:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT key '
                    'FROM base_table '
                    'WHERE key not in (SELECT key from validation_table)')
                flush_visited_point_id_timestamp()
                with VISITED_POINT_ID_TIMESTAMP_MAP_LOCK:
                    for payload in cursor:
                        unvalidated_point_id = payload[0]
                        if unvalidated_point_id not in (
                                VISITED_POINT_ID_TIMESTAMP_MAP):
                            break
        return process_point(unvalidated_point_id)
    except:
        LOGGER.exception('exception in unvalidated')
        raise

def flush_visited_point_id_timestamp():
    """Remove old entried in the visited unvalid map."""
    now = time.time()
    with VISITED_POINT_ID_TIMESTAMP_MAP_LOCK:
        global VISITED_POINT_ID_TIMESTAMP_MAP
        VISITED_POINT_ID_TIMESTAMP_MAP = {
            _point_id: _timestamp
            for _point_id, _timestamp in VISITED_POINT_ID_TIMESTAMP_MAP.items()
            if _timestamp+ACTIVE_DELAY > now
        }


@APP.route('/<int:point_id>')
def process_point(point_id):
    """Entry page."""
    LOGGER.debug('process point %s', point_id)
    flush_visited_point_id_timestamp()
    VISITED_POINT_ID_TIMESTAMP_MAP[point_id] = time.time()
    try:
        with DB_LOCK:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT source_id, source_key, source_point_wkt '
                    'from base_table WHERE key = ?', (point_id,))
                source_id, source_key, geometry_wkt = cursor.fetchone()
                base_point_geom = shapely.wkt.loads(geometry_wkt)
                base_point_id = f'{source_id}({source_key})'

                cursor.execute(
                    'SELECT bounding_box_bounds, metadata '
                    'from validation_table WHERE key = ?', (point_id,))
                payload = cursor.fetchone()

                # make a default metadata object just in case it's not defined
                metadata = {
                    'comments': DEFAULT_COMMENT_BOX_TEXT,
                    }
                bounding_box_bounds = None
                checkbox_values = {}
                if payload is not None:
                    bounding_box_bounds, metadata_json = payload
                    if metadata_json is not None:
                        metadata = json.loads(metadata_json)
                        if 'checkbox_values' in metadata:
                            checkbox_values = metadata['checkbox_values']
                        if 'comments' not in metadata:
                            metadata['comments'] = DEFAULT_COMMENT_BOX_TEXT

        return flask.render_template(
            'validation.html', **{
                'point_id': point_id,
                'base_point_id': base_point_id,
                'base_point_geom': base_point_geom,
                'bounding_box_bounds': bounding_box_bounds,
                'default_comments_text': DEFAULT_COMMENT_BOX_TEXT,
                'stored_comments_text': metadata['comments'],
                'checkbox_values': checkbox_values,
            })
    except Exception as e:
        LOGGER.exception('exception in process point')
        return str(e)


@APP.route('/markermove', methods=['POST'])
def move_marker():
    """Push event on a marker."""
    try:
        LOGGER.debug('got a post')
        payload = json.loads(flask.request.data.decode('utf-8'))
        LOGGER.debug(payload)
        bounding_box_bounds = None
        if 'bounding_box_bounds' in payload:
            bounding_box_bounds = [
                payload['bounding_box_bounds']['_southWest'],
                payload['bounding_box_bounds']['_northEast']]
        with DB_LOCK:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT OR REPLACE INTO validation_table '
                    'VALUES (?, ?, ?)',
                    (str(bounding_box_bounds), payload['point_id'],
                     json.dumps(payload['metadata'])))
        LOGGER.debug('move marker')
        return 'good'
    except:
        LOGGER.exception("big error")
        return 'error'


def build_base_validation_db(
        database_info_map, target_database_path, complete_token_path):
    """Build the base database for validation.

    Parameters:
        task_graph (TaskGraph): to avoid re-downloads and extractions of the
            database.
        database_info_map: map of "database id"s to 'database_url', and
            'database_expected_path' locations.
        list of (vector_path, key) pairs
            that should be ingested into the target database. the
            `vector_path` component refers to a vector geometry path that
            has a keyfield `key` to uniquely identify the geometry.
        target_database_path (str): path to a target database that contains
            a table called 'base_table' with columns:
                * data_source (original path)
                * data_key (original key to feature)
                * key (unique key)
            and a table called 'validation_table' with columns:
                * key (remote to 'base_table')
                * bounding_box_bounds text (wkt of moved point)
        complete_token_path (str): path to file that will be created if



    Returns:
        None.
    """
    sql_create_projects_table = (
        """
        CREATE TABLE IF NOT EXISTS base_table (
            database_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            source_point_wkt TEXT NOT NULL,
            key INTEGER NOT NULL PRIMARY KEY
        );
        CREATE UNIQUE INDEX IF NOT EXISTS base_table_index
        ON base_table (key);

        CREATE TABLE IF NOT EXISTS validation_table (
            bounding_box_bounds TEXT,
            key INTEGER NOT NULL PRIMARY KEY,
            metadata TEXT,
            FOREIGN KEY (key) REFERENCES base_table(key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS validation_table_index
        ON validation_table (key);
        """)
    with DB_LOCK:
        with sqlite3.connect(target_database_path) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_create_projects_table)

            next_feature_id = 0
            for database_id, database_map in database_info_map.items():
                # fetch the database and unzip it
                target_path = os.path.join(
                    WORKSPACE_DIR,
                    os.path.basename(database_map['database_url']))
                token_file = f'{target_path}.UNZIPPED'
                download_and_unzip(
                    database_map['database_url'], target_path, token_file)
                vector = gdal.OpenEx(
                    database_map['database_expected_path'], gdal.OF_VECTOR)
                layer = vector.GetLayer()
                for feature in layer:
                    geom = feature.GetGeometryRef()
                    key_val = feature.GetField(database_map['database_key'])
                    cursor.execute(
                        'INSERT OR IGNORE INTO base_table VALUES (?, ?, ?, ?)',
                        (database_id, key_val, geom.ExportToWkt(), next_feature_id))
                    next_feature_id += 1

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def download_and_unzip(url, target_path, token_file):
    """Download url to target and write a token file when it unzips."""
    reproduce.utils.url_fetch_and_validate(url, target_path)
    with zipfile.ZipFile(target_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(target_path))


def init():
    """Initialize system."""


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)
    DB_LOCK = threading.Lock()
    VISITED_POINT_ID_TIMESTAMP_MAP_LOCK = threading.Lock()
    complete_token_path = os.path.join(os.path.dirname(
        DATABASE_PATH), f'{os.path.basename(DATABASE_PATH)}_COMPLETE')
    TASK_GRAPH.add_task(
        func=build_base_validation_db,
        args=(POINT_DAM_DATA_MAP, DATABASE_PATH, complete_token_path),
        target_path_list=[complete_token_path],
        task_name='build the dam database')
    APP.run(host='0.0.0.0', port=8080)
