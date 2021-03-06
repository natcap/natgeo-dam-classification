"""Flask app to validate imagery and point locations."""
import queue
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
VALIDATAION_WORKER_DIED = False

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
logging.getLogger('taskgraph').setLevel(logging.INFO)

APP = Flask(__name__, static_url_path='', static_folder='')
APP.config['SECRET_KEY'] = b'\xe2\xa9\xd2\x82\xd5r\xef\xdb\xffK\x97\xcfM\xa2WH'
VISITED_POINT_ID_TIMESTAMP_MAP = {}
ACTIVE_USERS_MAP = {}
WORKSPACE_DIR = 'workspace'
TIME_TO_KEEP_ACTIVE_USERS = 5*60
_ZERO_USER = 'zero_bot'

VALIDATION_DATABASE_PATH = os.path.join(WORKSPACE_DIR)
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'dam_bounding_box_db.db')
N_WORKERS = -1
REPORTING_INTERVAL = 5.0
DEFAULT_COMMENT_BOX_TEXT = '(optional comments)'
DEFAULT_NAME_TEXT = '(enter your name or initials)'
ACTIVE_DELAY = 30.0  # wait this many seconds before trying point again
MAX_ATTEMPTS = 10


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


def parse_south_africa_database(exclude_filter_tuple):
    def _parse_south_africa_database(base_path):
        """Extract db key, description, and geom from base path.

        Parameters:
            base_path (str): path to a database, may be gdal vector, excel, or
                more.

        Returns:
            a list of (base_db_key, description, geom) tuples from the data in
            this database.

        """
        result_list = []
        df = pandas.read_csv(base_path, encoding='latin1')
        df = df.dropna(subset=[exclude_filter_tuple[0]])
        if exclude_filter_tuple is not None:
            LOGGER.debug(exclude_filter_tuple[0])
            LOGGER.debug(df[exclude_filter_tuple[0]].str)
            LOGGER.debug(
                df[exclude_filter_tuple[0]].str.contains('|'.join(
                    exclude_filter_tuple[1])))
            df = df[~df[exclude_filter_tuple[0]].str.contains('|'.join(
                exclude_filter_tuple[1]))]
        result = df[
                ['No of dam',
                 'Name of dam',
                 'Latitude deg',
                 'Lat min',
                 'Lat sec',
                 'Longitude deg',
                 'Long min',
                 'Long sec']].to_dict('records')

        # convert to result list and make wkt points
        result_list = [
            (index,
             db['Name of dam'],
             shapely.geometry.Point(
                db['Longitude deg'] +
                db['Long min']/60. +
                db['Long sec']/3600.,
                -(db['Latitude deg'] +
                  db['Lat min']/60. +
                  db['Lat sec']/3600.)).wkt)
            for index, db in enumerate(result)]
        LOGGER.debug(result_list)
        LOGGER.debug(len(result_list))
        return result_list
    return _parse_south_africa_database


def parse_pandas(
        db_key, description_key, lat_lng_key_tuple, include_filter_tuple,
        exclude_filter_tuple, encoding, pandas_type):
    """Create closure to extract db key, description, and geom from base path.

    Parameters:
        db_key (str): field that identifies the unique key in this database
            if None, will use FID
        description_key (str): field that identifies the dam description in
            this database
        lat_lng_key_tuple (tuple): the string keys for lat/lng identification
        include_filter_tuple (tuple): tuple of 'ID' to filter on, then a tuple
            of strings to match.
        exclude_filter_tuple (tuple): tuple of 'ID' to filter on, then a tuple
            of strings to match.
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
        if include_filter_tuple is not None:
            df = df.dropna(subset=[
                include_filter_tuple[0],
                lat_lng_key_tuple[0], lat_lng_key_tuple[1]])
            df = df[df[include_filter_tuple[0]].str.contains('|'.join(
                include_filter_tuple[1]))]

        if exclude_filter_tuple is not None:
            df = df.dropna(subset=[
                exclude_filter_tuple[0],
                lat_lng_key_tuple[0], lat_lng_key_tuple[1]])
            df = df[~df[exclude_filter_tuple[0]].str.contains('|'.join(
                exclude_filter_tuple[1]))]
        if db_key is None:
            result = df[
                [description_key, lat_lng_key_tuple[0],
                 lat_lng_key_tuple[1]]].to_dict('records')
        else:
            result = df[
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
SOUTH_AFRICA_DATABASE_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/south_africa_database_filtered_by_RS_md5_41a5504f2dde78d63e7c5fdb17940afb.csv'
GRAND_VERSION_1_3_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/GRanD_Version_1_3_md5_03968a0771b2981a36946848a9f2485c.zip'
CADASTRO_URL = 'https://storage.googleapis.com/natcap-natgeo-dam-ecoshards/cadastroRSB2017_filtered_by_RS_md5_7a332684b686308280a2882926694e4f.csv'


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
            None, 'HYDRO_NAME', ('LON', 'LAT'), None, None, 'latin1', 'csv')
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
            ('Status', ('C',)), None, None, 'xlsx')
        }),
    ('Greater Mekong Hydropower Database', {
        'database_url': GREATER_MEKONG_HYDROPOWER_DATABASE_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(
                GREATER_MEKONG_HYDROPOWER_DATABASE_URL)),
        'parse_function': parse_pandas(
            None, 'Project name', ('Long', 'Lat'),
            ('Status', ('COMM', 'OP')), None, 'latin1', 'csv'),
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
            None, ('PURPOSES', ('N', 'D')), None, 'xlsx')
    }),
    ('Cadastro Database', {
        'database_url': CADASTRO_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(
                CADASTRO_URL)),
        'parse_function': parse_pandas(
            None, 'Barragem_Nome', ('Longitude_dec', 'Latitude_dec'),
            None,
            ('Uso_principal', (
                "Contenção de resíduos industriais",
                "Contenção de rejeitos de mineração")),
            'latin1', 'csv')
    }),
    ('GRAND', {
        'database_url': GRAND_VERSION_1_3_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, 'GRanD_Version_1_3/GRanD_dams_v1_3.shp'),
        'parse_function': parse_shapefile(
            'GRAND_ID', 'DAM_NAME', None),
    }),
    ('South Africa Database', {
        'database_url': SOUTH_AFRICA_DATABASE_URL,
        'database_expected_path': os.path.join(
            WORKSPACE_DIR, os.path.basename(
                SOUTH_AFRICA_DATABASE_URL)),
        'parse_function': parse_south_africa_database(
            ('Purpose', (
                "INDUSTRIAL RESIDUE",
                "MINE RESIDUE",
                "ASH RESIDUE"
                "OXIDATION/EVAPORATION"
                "OXIDATION / EVAPORATION")))
    }),
)

@APP.route('/favicon.ico')
def favicon():
    return flask.send_from_directory(
        os.path.join(APP.root_path, 'images'), 'favicon.ico',
        mimetype='image/vnd.microsoft.icon')

@APP.route('/')
def get_unvalidated_point():
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    """Get a point that has not been validated."""
    LOGGER.info('trying to get an unvalidated point')
    point_id_to_process = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        # Try to get an unvalidated non NID point
        flush_visited_point_id_timestamp()
        while True:
            # scrub any 0 points.
            cursor.execute(
                "SELECT key, source_point_wkt "
                "FROM base_table "
                "WHERE key not in (SELECT key from validation_table) AND "
                "source_point_wkt LIKE '%(0 0)%';")
            for payload in cursor:
                zero_point_id = payload[0]
                source_point_wkt = payload[1]
                source_point = shapely.wkt.loads(source_point_wkt)
                if source_point.x == 0 and source_point.y == 0:
                    # insert bad point
                    payload = {
                        'point_id': zero_point_id,
                        'metadata': {
                            'comments': (
                                'automated removal of a (0, 0) '
                                'coordinate dam')
                        }
                    }
                    VALIDATION_INSERT_QUEUE.put((payload, _ZERO_USER))
            # Lisa wants to do the us national inventory of dams last
            cursor.execute(
                "SELECT key, source_point_wkt "
                "FROM base_table "
                "WHERE key not in (SELECT key from validation_table) AND "
                "database_id != 'US National Inventory of Dams' "
                "ORDER BY RANDOM();")
            with VISITED_POINT_ID_TIMESTAMP_MAP_LOCK:
                for payload in cursor:
                    unvalidated_point_id = payload[0]
                    source_point_wkt = payload[1]
                    source_point = shapely.wkt.loads(source_point_wkt)
                    if unvalidated_point_id not in (
                            VISITED_POINT_ID_TIMESTAMP_MAP):
                        point_id_to_process = unvalidated_point_id
                        break
            if point_id_to_process is None:
                LOGGER.info('must have nothing but NID left.')
                cursor.execute(
                    'SELECT key, source_point_wkt '
                    'FROM base_table '
                    'WHERE key not in (SELECT key from validation_table) '
                    'ORDER BY RANDOM();')
                with VISITED_POINT_ID_TIMESTAMP_MAP_LOCK:
                    for payload in cursor:
                        unvalidated_point_id = payload[0]
                        source_point_wkt = payload[1]
                        source_point = shapely.wkt.loads(source_point_wkt)
                        if unvalidated_point_id not in (
                                VISITED_POINT_ID_TIMESTAMP_MAP):
                            point_id_to_process = unvalidated_point_id
                            break
            break
        cursor.close()
        connection.commit()
        return process_point(unvalidated_point_id)
    except:
        LOGGER.exception('exception in unvalidated')
        raise


# Used to highlight different user's contributions
_KELLY_COLORS_HEX = [
    '#FFB300',  # Vivid Yellow
    '#803E75',  # Strong Purple
    '#FF6800',  # Vivid Orange
    '#A6BDD7',  # Very Light Blue
    '#C10020',  # Vivid Red
    '#CEA262',  # Grayish Yellow
    '#817066',  # Medium Gray
    # The following don't work well for people with defective color vision
    '#007D34',  # Vivid Green
    '#F6768E',  # Strong Purplish Pink
    '#00538A',  # Strong Blue
    '#FF7A5C',  # Strong Yellowish Pink
    '#53377A',  # Strong Violet
    '#FF8E00',  # Vivid Orange Yellow
    '#B32851',  # Strong Purplish Red
    '#F4C800',  # Vivid Greenish Yellow
    '#7F180D',  # Strong Reddish Brown
    '#93AA00',  # Vivid Yellowish Green
    '#593315',  # Deep Yellowish Brown
    '#F13A13',  # Vivid Reddish Orange
    '#232C16',  # Dark Olive Green
    ]


@APP.route('/summary')
def render_summary():
    """Get a point that has not been validated."""
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute('SELECT count(1) FROM base_table')
        total_count = cursor.fetchone()[0]

        cursor.execute(
            'SELECT username, count(username) '
            'FROM validation_table '
            'GROUP by username '
            'ORDER BY count(username) DESC')
        user_color_point_list = []
        for user_color, (username, user_count) in zip(
                _KELLY_COLORS_HEX, cursor.fetchall()):
            cursor.execute(
                'SELECT source_point_wkt '
                'FROM base_table '
                'WHERE key in ('
                'SELECT key from validation_table '
                'WHERE username = ?)', (username,))
            user_point_list = [
                shapely.wkt.loads(wkt[0]) for wkt in cursor]
            user_valid_point_list = [
                (point.y, point.x) for point in user_point_list]
            user_color_point_list.append(
                (user_color, user_valid_point_list))
        cursor.close()
        connection.commit()
        return flask.render_template(
            'summary.html', **{
                'total_count': total_count,
                'database_list': POINT_DAM_DATA_MAP_LIST,
                'user_color_point_list': user_color_point_list
            })
    except:
        LOGGER.exception('exception render_summary')
        raise

@APP.route('/calculate_user_contribution', methods=['GET'])
def calculate_user_contribution():
    """Return a list of color username/count tuples."""
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(
            'SELECT username, count(username) '
            'FROM validation_table '
            'GROUP by username '
            'ORDER BY count(username) DESC')
        user_contribution_list = []
        for user_color, (username, user_count) in zip(
                _KELLY_COLORS_HEX, cursor.fetchall()):
            user_contribution_list.append(
                (user_color, username, user_count))

        # rows validated
        cursor.execute(
            'SELECT count(1) '
            'FROM base_table '
            'WHERE key not in (SELECT key from validation_table)')
        unvalidated_count = cursor.fetchone()[0]
        cursor.execute('SELECT count(1) FROM base_table')
        total_count = cursor.fetchone()[0]

        cursor.execute(
            'SELECT count(1) from validation_table '
            'WHERE bounding_box_bounds != "None";')
        count_with_bounding_box = int(cursor.fetchone()[0])
        cursor.close()
        connection.commit()
        percent_with_bounding_box = (
            100.0 * count_with_bounding_box / total_count)
        return json.dumps({
            'user_contribution_list': user_contribution_list,
            'dams_validated': total_count-unvalidated_count,
            'percent_dams_validated': '%.2f%%' % (
                100.0*(total_count-unvalidated_count) / total_count),
            'dams_with_bounding_box': count_with_bounding_box,
            'percent_dams_with_bounding_box': '%.2f%%' % (
                percent_with_bounding_box)
        })
    except Exception as e:
        return str(e)


@APP.route('/user_validation_summary', methods=['GET'])
def user_validation_summary():
    """Dump a table of classified info."""
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        # rows validated
        cursor.execute(
            'SELECT '
            'database_id, source_key, '
            'description, validation_table.key, '
            'validation_table.metadata, validation_table.username '
            'FROM base_table '
            'INNER JOIN validation_table on  '
            'validation_table.key = base_table.key;')
        key_metadata_list = list(cursor.fetchall())
        cursor.close()
        connection.commit()

        return flask.render_template(
            'user_validation_summary.html', **{
                'key_metadata_list': key_metadata_list
            })
    except:
        LOGGER.exception('exception classification_summary')
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
        for username in list(ACTIVE_USERS_MAP):
            if now - ACTIVE_USERS_MAP[username][1] > TIME_TO_KEEP_ACTIVE_USERS:
                del ACTIVE_USERS_MAP[username]


@APP.route('/<int:point_id>')
def process_point(point_id):
    """Entry page."""
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    LOGGER.info('process point %s', point_id)
    flush_visited_point_id_timestamp()
    VISITED_POINT_ID_TIMESTAMP_MAP[point_id] = time.time()
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
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
        cursor.close()
        connection.commit()

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


@APP.route('/active_users', methods=['GET'])
def active_users():
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    flush_visited_point_id_timestamp()
    return json.dumps(ACTIVE_USERS_MAP)


@APP.route('/update_username', methods=['POST'])
def update_username():
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    try:
        payload = json.loads(flask.request.data.decode('utf-8'))
        flask.session['username'] = payload['username']
        return flask.session['username']
    except:
        LOGGER.exception('error')
        return 'error'


@APP.route('/update_dam_data', methods=['POST'])
def update_dam_data():
    """Push event on a marker."""
    if VALIDATAION_WORKER_DIED:
        return "VALIDATAION WORKER DIED. TELL LISA!"
    try:
        LOGGER.info('got a post to update dam database')
        payload = json.loads(flask.request.data.decode('utf-8'))
        VALIDATION_INSERT_QUEUE.put(
            (payload, flask.session['username']))
        return flask.jsonify(success=True)
    except:
        LOGGER.exception("big error")
        return flask.jsonify(success=False)


def validation_queue_worker():
    """Process validation queue."""
    try:
        while True:
            payload, username = VALIDATION_INSERT_QUEUE.get()
            username = username.replace('\n','').rstrip()
            if username not in ACTIVE_USERS_MAP:
                ACTIVE_USERS_MAP[username] = (1, time.time())
            else:
                ACTIVE_USERS_MAP[username] = (
                    ACTIVE_USERS_MAP[username][0]+1, time.time())
            bounding_box_bounds = None
            if 'bounding_box_bounds' in payload:
                bounding_box_bounds = [
                    payload['bounding_box_bounds']['_southWest'],
                    payload['bounding_box_bounds']['_northEast']]
            attempts = 0
            while True:
                try:
                    connection = get_db_connection()
                    cursor = connection.cursor()
                    cursor.execute('SELECT max(id) FROM validation_table;')
                    max_validation_id = cursor.fetchone()[0]
                    if max_validation_id is None:
                        max_validation_id = -1
                    cursor.execute(
                        'INSERT OR REPLACE INTO validation_table '
                        'VALUES (?, ?, ?, ?, ?, ?);',
                        (str(bounding_box_bounds), payload['point_id'],
                         json.dumps(payload['metadata']),
                         username,
                         str(datetime.datetime.utcnow()),
                         max_validation_id+1))
                    cursor.close()
                    connection.commit()
                    break
                except sqlite3.OperationalError:
                    if attempts > MAX_ATTEMPTS:
                        raise
                    LOGGER.exception(
                        f"attempted {attempts} times, trying again")
    except:
        LOGGER.exception('validation queue worker crashed.')
        global VALIDATAION_WORKER_DIED
        VALIDATAION_WORKER_DIED = True


@APP.after_request
def add_header(r):
    """Force no caching."""
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


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
            id INTEGER NOT NULL UNIQUE,
            FOREIGN KEY (key) REFERENCES base_table(key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS validation_table_index
        ON validation_table (key);
        CREATE UNIQUE INDEX IF NOT EXISTS validation_table_id_index
        ON validation_table (id);
        """)
    connection = get_db_connection()
    cursor = connection.cursor()
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
    cursor.close()
    connection.commit()

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def download_and_unzip(url, target_path, token_file):
    """Download url to target and write a token file when it unzips."""
    if not os.path.exists(target_path):
        reproduce.utils.url_fetch_and_validate(url, target_path)
        if target_path.endswith('zip'):
            with zipfile.ZipFile(target_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(target_path))


def get_db_connection():
    thread_id = threading.get_ident()
    if thread_id not in DB_CONN_THREAD_MAP:
        DB_CONN_THREAD_MAP[thread_id] = sqlite3.connect(DATABASE_PATH)
    connection = DB_CONN_THREAD_MAP[thread_id]
    return connection


if __name__ == '__main__':
    DB_CONN_THREAD_MAP = {}
    TASK_GRAPH = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)
    VALIDATION_INSERT_QUEUE = queue.Queue()
    validation_thread = threading.Thread(
        target=validation_queue_worker)
    validation_thread.start()
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
    APP.run(host='0.0.0.0', port=8888)
    # this makes a connection per thread
