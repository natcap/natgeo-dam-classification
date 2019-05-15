"""Flask app to validata imagery and point locations."""
import requests
import queue
import urllib
import json
import datetime
import sqlite3
import os
import sys
import logging
import threading

import pygeoprocessing
from retrying import retry
import numpy
import taskgraph
import shapely.wkt
import shapely.geometry
from osgeo import gdal
from flask import Flask
import flask
from osgeo import gdal


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
WORKSPACE_DIR = 'workspace_not_a_dam'
PLANET_QUADS_DIR = os.path.join(WORKSPACE_DIR, 'planet_quads')
DAM_IMAGERY_DIR = os.path.join(WORKSPACE_DIR, 'dam_images')
GSW_DIR = os.path.join(WORKSPACE_DIR, 'gsw_tiles')
PLANET_STITCHED_IMAGERY_DIR = os.path.join(PLANET_QUADS_DIR, 'stiched_images')
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'not_a_dam.db')
DAM_STATUS_DB_PATH = os.path.join(WORKSPACE_DIR, 'dam_status.db')
PLANET_API_KEY_FILE = 'planet_api_key.txt'
ACTIVE_MOSAIC_JSON_PATH = os.path.join(WORKSPACE_DIR, 'active_mosaic.json')
N_WORKERS = -1
REQUEST_TIMEOUT = 1.0
REPORTING_INTERVAL = 5.0
NOT_A_DAM_IMAGES_TO_CACHE = 10
MAX_GSW_TRIES = 4096
BOUNDING_BOX_SIZE_M = 2000.0
PLANET_QUAD_CELL_SIZE = 4.77731
MIN_SURFACE_WATER = 20
MAX_SURFACE_WATER = 80



@APP.route('/favicon.ico')
def favicon():
    return flask.send_from_directory(
        os.path.join(APP.root_path, 'images'), 'favicon.ico',
        mimetype='image/vnd.microsoft.icon')

@APP.route('/')
def get_unvalidated_image():
    try:
        #connection = get_db_connection()
        #cursor = connection.cursor()
        return flask.render_template(
            'not_a_dam_validation.html', **{
            })
    except:
        LOGGER.exception('something bad happened')


@APP.route('/update_is_a_dam', methods=['POST'])
def update_is_a_dam():
    """Called when there is a dam image that's classified."""
    payload = json.loads(flask.request.data.decode('utf-8'))
    LOGGER.debug(payload)
    return flask.jsonify({'image_url': 'image_url_goes_here'})


@APP.route('/summary')
def render_summary():
    """Get a point that has not been validated."""
    return 'summary page'


def download_url_op(url, target_path, skip_if_target_exists=False):
    """Download `url` to `target_path`."""
    if skip_if_target_exists and os.path.exists(target_path):
        LOGGER.info('target exists %s', target_path)
        return
    with open(target_path, 'wb') as target_file:
        url_stream = urllib.request.urlopen(url)
        meta = url_stream.info()
        file_size = int(meta["Content-Length"])
        LOGGER.info(
            "Downloading: %s Bytes: %s" % (target_path, file_size))
        downloaded_so_far = 0
        block_size = 2**20
        while True:
            data_buffer = url_stream.read(block_size)
            if not data_buffer:
                break
            downloaded_so_far += len(data_buffer)
            target_file.write(data_buffer)
            status = r"%s: %10d [%3.2f%%]" % (
                os.path.basename(target_path),
                downloaded_so_far, downloaded_so_far * 100. / file_size)
            LOGGER.info(status)


def image_candidate_worker():
    """Process validation queue."""
    try:
        while True:
            n_dams_to_fetch = IMAGE_CANDIDATE_QUEUE.get()
            if n_dams_to_fetch == 'STOP':
                return
            for _ in range(n_dams_to_fetch):
                lng = numpy.random.random()*360-180
                lat = numpy.random.random()*180-90

                # take the ceil to the nearest 10
                lng = int(numpy.floor(lng*0.1)*10)
                if lng < 0:
                    lng_dir = 'W'
                    lng = abs(lng)
                else:
                    lng_dir = 'E'

                # take the ceil to the nearest 10
                lat = int(numpy.ceil(lat*0.1)*10)
                if lat < 0:
                    lat_dir = 'S'
                    lat = abs(lat)
                else:
                    lat_dir = 'N'

                src_url = (
                    f'http://storage.googleapis.com/global-surface-water/'
                    f'downloads/occurrence/occurrence_'
                    f'{lng}{lng_dir}_{lat}{lat_dir}.tif')
                LOGGER.info("download a new GSW tile: %s", src_url)
                surface_water_raster_path = os.path.join(
                    GSW_DIR, os.path.basename(src_url))
                download_url_op(
                    src_url, surface_water_raster_path,
                    skip_if_target_exists=True)
                LOGGER.info('downloaded!')
                gsw_raster = gdal.Open(
                    surface_water_raster_path, gdal.OF_RASTER)
                gsw_band = gsw_raster.GetRasterBand(1)
                box_size = int((BOUNDING_BOX_SIZE_M / PLANET_QUAD_CELL_SIZE))

                tries = 0
                while True:
                    tries += 1
                    if tries >= MAX_GSW_TRIES:
                        break
                    # we expect the raster to be square so it's okay to use XSize for
                    # both dimensions so pick a point in the range of the quad
                    ul_x = int(numpy.random.randint(
                        0, gsw_band.XSize-box_size, dtype=numpy.int32))
                    ul_y = int(numpy.random.randint(
                        0, gsw_band.YSize-box_size, dtype=numpy.int32))
                    sample_block = gsw_band.ReadAsArray(
                        xoff=ul_x, yoff=ul_y, win_xsize=box_size,
                        win_ysize=box_size)
                    # search for pixels there that include edge surface water
                    partial_samples = numpy.argwhere(
                        (sample_block > MIN_SURFACE_WATER) &
                        (sample_block < MAX_SURFACE_WATER))

                    # if we found at least 20 percent of the pixels are
                    # partial water samples.
                    if partial_samples.size > (.2*sample_block.size):
                        break

                if tries >= MAX_GSW_TRIES:
                    LOGGER.error("COULDN'T FIND A BOUNDING BOX")
                    IMAGE_CANDIDATE_QUEUE.put(1)
                    continue

                LOGGER.info("now pull a planet quad")
                gsw_gt = pygeoprocessing.get_raster_info(
                    surface_water_raster_path)['geotransform']
                min_x, max_y = gdal.ApplyGeoTransform(gsw_gt, ul_x, ul_y)
                max_x, min_y = gdal.ApplyGeoTransform(
                    gsw_gt, ul_x+box_size, ul_y+box_size)

                mosaic_quad_response = get_bounding_box_quads(
                    SESSION, MOSAIC_QUAD_LIST_URL, min_x, min_y, max_x, max_y)
                mosaic_quad_response_dict = mosaic_quad_response.json()
                quad_download_dict = {
                    'quad_download_url_list':  [],
                    'quad_target_path_list': [],
                    'dam_lat_lng_bb': [min_x, min_y, max_x, max_y]
                }
                for mosaic_item in mosaic_quad_response_dict['items']:
                    quad_download_url = (mosaic_item['_links']['download'])
                    quad_download_raster_path = os.path.join(
                        PLANET_QUADS_DIR, active_mosaic['id'],
                        f'{mosaic_item["id"]}.tif')
                    quad_download_dict['quad_download_url_list'].append(
                        quad_download_url)
                    quad_download_dict['quad_target_path_list'].append(
                        quad_download_raster_path)
                    download_url_op(
                        quad_download_url, quad_download_raster_path,
                        skip_if_target_exists=True)


@APP.after_request
def add_header(r):
    """Force no caching."""
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


def build_image_db(target_database_path, complete_token_path):
    """Build the base database for validation.

    Parameters:
        target_database_path (str): path to a target database that contains
            a table called 'base_table' with columns:
                * image path
                * bounding_box_bounds text (wkt of geometry?)
                * dam_in_image (true/false/NULL) (null means not classified
                  yet)
        complete_token_path (str): path to file that will be created when the
            DB is first created. Used to guard taskgraph from remaking if the
            DB has changed.

    Returns:
        None.

    """
    sql_create_projects_table = (
        """
        CREATE TABLE IF NOT EXISTS base_table (
            image_path TEXT NOT NULL PRIMARY KEY,
            bounding_box TEXT NOT NULL,
            dam_in_image BOOL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS image_path_index
        ON base_table (image_path);
        """)
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.executescript(sql_create_projects_table)
    cursor.close()
    connection.commit()

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def get_db_connection():
    """Fetch the open database connection for this thread."""
    thread_id = threading.get_ident()
    if thread_id not in DB_CONN_THREAD_MAP:
        DB_CONN_THREAD_MAP[thread_id] = sqlite3.connect(DATABASE_PATH)
    connection = DB_CONN_THREAD_MAP[thread_id]
    return connection


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_bounding_box_quads(
        session, mosaic_quad_list_url, min_x, min_y, max_x, max_y):
    """Query for mosaic via bounding box and retry if necessary."""
    try:
        mosaic_quad_response = session.get(
            f'{mosaic_quad_list_url}?bbox={min_x},{min_y},{max_x},{max_y}',
            timeout=REQUEST_TIMEOUT)
        return mosaic_quad_response
    except:
        LOGGER.exception(
            f"get_bounding_box_quads {min_x},{min_y},{max_x},{max_y} failed")
        raise


if __name__ == '__main__':
    for dir_path in [PLANET_QUADS_DIR, DAM_IMAGERY_DIR, GSW_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    with open(PLANET_API_KEY_FILE, 'r') as planet_api_key_file:
        planet_api_key = planet_api_key_file.read().rstrip()

    SESSION = requests.Session()
    SESSION.auth = (planet_api_key, '')

    if not os.path.exists(ACTIVE_MOSAIC_JSON_PATH):
        mosaics_json = SESSION.get(
            'https://api.planet.com/basemaps/v1/mosaics',
            timeout=REQUEST_TIMEOUT)
        most_recent_date = ''
        active_mosaic = None
        for mosaic_data in mosaics_json.json()['mosaics']:
            if mosaic_data['interval'] != '3 mons':
                continue
            last_acquired_date = mosaic_data['last_acquired']
            LOGGER.debug(last_acquired_date)
            if last_acquired_date > most_recent_date:
                most_recent_date = last_acquired_date
                active_mosaic = mosaic_data
        with open(ACTIVE_MOSAIC_JSON_PATH, 'w') as active_mosaic_file:
            active_mosaic_file.write(json.dumps(active_mosaic))
    else:
        with open(ACTIVE_MOSAIC_JSON_PATH, 'r') as active_mosaic_file:
            active_mosaic = json.load(active_mosaic_file)

    LOGGER.debug(
        'using this mosaic: '
        f"""{active_mosaic['last_acquired']} {active_mosaic['interval']} {
            active_mosaic['grid']['resolution']}""")

    MOSAIC_QUAD_LIST_URL = (
        f"""https://api.planet.com/basemaps/v1/mosaics/"""
        f"""{active_mosaic['id']}/quads""")

    DB_CONN_THREAD_MAP = {}
    TASK_GRAPH = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)
    IMAGE_CANDIDATE_QUEUE = queue.Queue()
    IMAGE_CANDIDATE_QUEUE.put(1)
    image_candidate_thread = threading.Thread(target=image_candidate_worker)
    image_candidate_thread.start()
    dabase_complete_token_path = os.path.join(os.path.dirname(
        DATABASE_PATH), f'{os.path.basename(DATABASE_PATH)}_COMPLETE')
    build_db_task = TASK_GRAPH.add_task(
        func=build_image_db,
        args=(DATABASE_PATH, dabase_complete_token_path),
        target_path_list=[dabase_complete_token_path],
        ignore_path_list=[DATABASE_PATH],
        task_name='build the dam database')
    build_db_task.join()

    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(
        "SELECT count(1) "
        "FROM base_table "
        "WHERE dam_in_image is NULL;")
    UNVALIDATED_IMAGE_COUNT = int(cursor.fetchone()[0])
    LOGGER.debug(UNVALIDATED_IMAGE_COUNT)
    cursor.close()
    connection.commit()

    APP.run(host='0.0.0.0', port=8080)
    # this makes a connection per thread
