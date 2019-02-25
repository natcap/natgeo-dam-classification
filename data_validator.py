"""Flask app to validata imagery and point locations."""
import re
import zipfile
import json
import datetime
import sqlite3
import os
import sys
import logging
import threading
import time

import pandas
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
APP.config['SECRET_KEY'] = b'\xe2\xa9\xd2\x82\xd5r\xef\xdb\xffK\x97\xcfM\xa2WH'
LOGGER.debug(APP.config['SECRET_KEY'])
VISITED_POINT_ID_TIMESTAMP_MAP = {}
WORKSPACE_DIR = 'workspace'


def parse_shapefile(db_key, description_key, filter_tuple):
    """Create closure to xtract db key, description, and geom from base path.

    Parameters:
        db_key (str): field that identifies the unique key in this database
            if None, will use FID
        description_key (str): field that identifies the dam description in
            this database
        filter_tuple (tuple): tuple of 'ID' to filter on, then a tuple of
            strings to match.

    Returns:
        a list of (base_db_key, description, geom) tuples from the data in
        this database.

    """
    def _parse_shapefile(base_path):
        """Extract db key, description, and geom from base path.

        Parameters:
            base_path (str): path to a database, may be gdal vector, excel, or
                more.

        Returns:
            a list of (base_db_key, description, geom) tuples from the data in
            this database.

        """
        result_list = []
        vector = gdal.OpenEx(base_path, gdal.OF_VECTOR)
        layer = vector.GetLayer()
        for feature in layer:
            geom = feature.GetGeometryRef()
            if filter_tuple is not None:
                filter_text = feature.GetField(filter_tuple[0])
                for filter_rule in filter_tuple[1]:
                    if filter_rule == filter_text:
                        continue
            result_list.append((
                feature.GetFID if db_key is None else feature.GetField(db_key),
                feature.GetField(description_key),
                geom.ExportToWkt()))

        return result_list
    return _parse_shapefile


def parse_pandas(
        db_key, description_key, lat_lng_key_tuple, filter_tuple, encoding,
        pandas_type):
    """Create closure to extract db key, description, and geom from base path.

    Parameters:
        db_key (str): field that identifies the unique key in this database
            if None, will use FID
        description_key (str): field that identifies the dam description in
            this database
        lat_lng_key_tuple (tuple): the string keys for lat/lng identification
        filter_tuple (tuple): tuple of 'ID' to filter on, then a tuple of
            strings to match.
        pandas_type (str): either 'csv' for 'xlsx' for parsing.

    Returns:
        a list of (base_db_key, description, geom) tuples from the data in
        this database.

    """
    def _parse_pandas(base_path):
        """Extract db key, description, and geom from base path.

        Parameters:
            base_path (str): path to a database, may be gdal vector, excel, or
                more.

        Returns:
            a list of (base_db_key, description, geom) tuples from the data in
            this database.

        """
        result_list = []
        if pandas_type == 'csv':
            df = pandas.read_csv(base_path, encoding=encoding)
        elif pandas_type == 'xlsx':
            df = pandas.read_excel(base_path)

        # filter out everything that's not at least hydropower
        if filter_tuple is not None:
            df = df.dropna(subset=[
                filter_tuple[0], lat_lng_key_tuple[0], lat_lng_key_tuple[1]])
            filtered_df = df[df[filter_tuple[0]].str.contains('|'.join(
                filter_tuple[1]))]
        else:
            filtered_df = df
        if db_key is None:
            result = filtered_df[
                [description_key, lat_lng_key_tuple[0],
                 lat_lng_key_tuple[1]]].to_dict('records')
        else:
            result = filtered_df[
                [db_key, description_key, lat_lng_key_tuple[0],
                 lat_lng_key_tuple[1]]].to_dict(
                    'records')

        # convert to result list and make wkt points
        result_list = [
            (index if db_key is None else db[db_key],
             db[description_key],
             shapely.geometry.Point(
                float(re.findall(r'-?\d+\.\d+', str(db[lat_lng_key_tuple[0]]))[0]),
                float(re.findall(r'-?\d+\.\d+', str(db[lat_lng_key_tuple[1]]))[0])).wkt)
            for index, db in enumerate(result)]
        return result_list
        return result_list
    return _parse_pandas


GRAND_VERSION_1_1_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/GRanD_Version_1_1_md5_9ad04293d056cd35abceb8a15b953fb8.zip'
USNID_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/NID2018_U_2019_02_10_md5_305b5bf747c95653725fecfee94bddf5.xlsx'
VOLTA_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/VoltaReservoirs_V1_md5_d756671c6c2cc42d34b5dfa1aa3e9395.zip'
GREATER_MEKONG_HYDROPOWER_DATABASE_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/greater_mekong_hydropower_dams_md5_c94ef3dab1171018ac2c7a1831fe0cc1.csv'
MEKONG_DAM_DATABASE_FROM_RAFA_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/MekongDamDatabasefromRafa_cleaned_by_rps_md5_e9852db07e734884107ace82ef1c9c96.xlsx'
MYANMAR_DAMS_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/myanmar_dams_md5_4a5e49a515d30ac0937c2a36b43dcdf8.zip'
UHE_AMAZONIA_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/Base-UHE-AMAZONIA_md5_accd10ef136bc16d1e235e084c706e1e.csv'

POINT_DAM_DATA_MAP_LIST = (
     ('GRAND', {
        'database_url': GRAND_VERSION_1_1_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, 'GRanD_Version_1_1/GRanD_dams_v1_1.shp'),
        'parse_function': parse_shapefile(
            'GRAND_ID', 'DAM_NAME', None),
        }),
    ('UHE Amazonia', {
        'database_url': UHE_AMAZONIA_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(UHE_AMAZONIA_URL)),
        'parse_function': parse_pandas(
            None, 'HYDRO_NAME', ('LON', 'LAT'), None, 'latin1', 'csv')
        }),
    ('Myanmar Dams', {
        'database_url': MYANMAR_DAMS_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, 'myanmar_dams.shp'),
        'parse_function': parse_shapefile(
            'Project ID', 'Project Na', ('Status', ('Complete',)))
        }),
    ('Mekong dam database from Rafa', {
        'database_url': MEKONG_DAM_DATABASE_FROM_RAFA_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(
                MEKONG_DAM_DATABASE_FROM_RAFA_URL)),
        'parse_function': parse_pandas(
            'Code', 'Name', ('Lon1', 'Lat1'),
            ('Status', ('C',)), None, 'xlsx')
        }),
    ('Greater Mekong Hydropower Database', {
        'database_url': GREATER_MEKONG_HYDROPOWER_DATABASE_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(
                GREATER_MEKONG_HYDROPOWER_DATABASE_URL)),
        'parse_function': parse_pandas(
            None, 'Project name', ('Long', 'Lat'),
            ('Status', ('COMM', 'OP')), 'latin1', 'csv'),
        }),
    ('Volta', {
        'database_url': VOLTA_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, 'VoltaReservoirs_V1.shp'),
        'parse_function': parse_shapefile(
            'KCL_ID', 'KCL_ID', None),
        }),
    ('US National Inventory of Dams', {
        'database_url': USNID_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(USNID_URL)),
        'parse_function': parse_pandas(
            'NIDID', 'DAM_NAME', ('LONGITUDE', 'LATITUDE'),
            ('PURPOSES', ('H',)), None, 'xlsx')
    }),
)

VALIDATION_DATABASE_PATH = os.path.join(WORKSPACE_DIR)
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'dam_bounding_box_db.db')
N_WORKERS = -1
REPORTING_INTERVAL = 5.0
DEFAULT_COMMENT_BOX_TEXT = '(optional comments)'
DEFAULT_NAME_TEXT = '(enter your name or initials)'
ACTIVE_DELAY = 30.0  # wait this many seconds before trying point again


@APP.route('/favicon.ico')
def favicon():
    return flask.send_from_directory(
        os.path.join(APP.root_path, 'images'), 'favicon.ico',
        mimetype='image/vnd.microsoft.icon')

@APP.route('/')
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
                    'WHERE key not in (SELECT key from validation_table) '
                    'ORDER BY RANDOM() LIMIT 1;')
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


@APP.route('/summary')
def render_summary():
    """Get a point that has not been validated."""
    try:
        with DB_LOCK:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.cursor()
                # rows validated
                cursor.execute(
                    'SELECT count(1) '
                    'FROM base_table '
                    'WHERE key not in (SELECT key from validation_table)')
                unvalidated_count = cursor.fetchone()[0]
                cursor.execute('SELECT count(1) FROM base_table')
                total_count = cursor.fetchone()[0]

                cursor.execute(
                    'SELECT username, count(username) '
                    'FROM validation_table '
                    'GROUP by username '
                    'ORDER BY count(username) DESC')
                user_contribution_list = cursor.fetchall()

                cursor.execute(
                    'SELECT source_point_wkt '
                    'FROM base_table '
                    'WHERE key in (SELECT key from validation_table)')
                point_list = [
                    shapely.wkt.loads(wkt[0]) for wkt in cursor]
                valid_point_list = [
                    (point.y, point.x) for point in point_list]

        return flask.render_template(
            'summary.html', **{
                'unvalidated_count': unvalidated_count,
                'total_count': total_count,
                'database_list': POINT_DAM_DATA_MAP_LIST,
                'user_contribution_list': user_contribution_list,
                'valid_point_list': valid_point_list
            })
    except:
        LOGGER.exception('exception render_summary')
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
                    'SELECT '
                    'database_id, source_key, description, source_point_wkt '
                    'from base_table WHERE key = ?', (point_id,))
                database_id, source_key, dam_description, geometry_wkt = (
                    cursor.fetchone())
                base_point_geom = shapely.wkt.loads(geometry_wkt)
                base_point_id = f'{database_id}({source_key})'

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

        if 'username' in flask.session:
            session_username_text = flask.session['username']
        else:
            session_username_text = DEFAULT_NAME_TEXT

        return flask.render_template(
            'validation.html', **{
                'point_id': point_id,
                'dam_description': dam_description,
                'base_point_id': base_point_id,
                'base_point_geom': base_point_geom,
                'bounding_box_bounds': bounding_box_bounds,
                'default_comments_text': DEFAULT_COMMENT_BOX_TEXT,
                'default_name_text': DEFAULT_NAME_TEXT,
                'session_username_text': session_username_text,
                'stored_comments_text': metadata['comments'],
                'checkbox_values': checkbox_values,
            })
    except Exception as e:
        LOGGER.exception('exception in process point')
        return str(e)


@APP.route('/update_username', methods=['POST'])
def update_username():
    try:
        LOGGER.debug('change in username')
        payload = json.loads(flask.request.data.decode('utf-8'))
        flask.session['username'] = payload['username']
        return flask.session['username']
    except:
        LOGGER.exception('error')
        return 'error'

@APP.route('/update_dam_data', methods=['POST'])
def update_dam_data():
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
                    'VALUES (?, ?, ?, ?, ?)',
                    (str(bounding_box_bounds), payload['point_id'],
                     json.dumps(payload['metadata']),
                     flask.session['username'],
                     str(datetime.datetime.utcnow())))
        LOGGER.debug('move marker')
        return 'good'
    except:
        LOGGER.exception("big error")
        return 'error'


def build_base_validation_db(
        database_info_map_list, target_database_path, complete_token_path):
    """Build the base database for validation.

    Parameters:
        task_graph (TaskGraph): to avoid re-downloads and extractions of the
            database.
        database_info_map_list: list of map of (database id/database map)
            tuples to 'database_url', 'database_key', 'description_key'
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
            description TEXT NOT NULL,
            source_point_wkt TEXT NOT NULL,
            key INTEGER NOT NULL PRIMARY KEY
        );

        CREATE UNIQUE INDEX IF NOT EXISTS base_table_index
        ON base_table (key);

        CREATE TABLE IF NOT EXISTS validation_table (
            bounding_box_bounds TEXT,
            key INTEGER NOT NULL PRIMARY KEY,
            metadata TEXT,
            username TEXT,
            time_date TEXT,
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
            for database_id, database_map in database_info_map_list:
                # fetch the database and unzip it
                LOGGER.info('processing %s', database_id)
                target_path = os.path.join(
                    WORKSPACE_DIR,
                    os.path.basename(database_map['database_url']))
                token_file = f'{target_path}.UNZIPPED'
                download_and_unzip(
                    database_map['database_url'], target_path, token_file)
                dam_data_list = database_map['parse_function'](
                    database_map['database_expected_path'])
                for base_db_key, description, geom_wkt in dam_data_list:
                    cursor.execute(
                        'INSERT OR IGNORE INTO base_table '
                        'VALUES (?, ?, ?, ?, ?)',
                        (database_id, base_db_key, description,
                         geom_wkt, next_feature_id))
                    next_feature_id += 1

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def download_and_unzip(url, target_path, token_file):
    """Download url to target and write a token file when it unzips."""
    if not os.path.exists(target_path):
        reproduce.utils.url_fetch_and_validate(url, target_path)
        if target_path.endswith('zip'):
            with zipfile.ZipFile(target_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(target_path))


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)
    DB_LOCK = threading.Lock()
    VISITED_POINT_ID_TIMESTAMP_MAP_LOCK = threading.Lock()
    complete_token_path = os.path.join(os.path.dirname(
        DATABASE_PATH), f'{os.path.basename(DATABASE_PATH)}_COMPLETE')
    expected_database_path_list = [
        db_map['database_expected_path']
        for _, db_map in POINT_DAM_DATA_MAP_LIST]
    TASK_GRAPH.add_task(
        func=build_base_validation_db,
        args=(POINT_DAM_DATA_MAP_LIST, DATABASE_PATH, complete_token_path),
        target_path_list=[complete_token_path],
        ignore_path_list=expected_database_path_list+[DATABASE_PATH],
        task_name='build the dam database')
    APP.run(host='0.0.0.0', port=8080)
