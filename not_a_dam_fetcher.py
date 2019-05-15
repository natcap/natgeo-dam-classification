"""Flask app to validata imagery and point locations."""
import queue
import urllib
import json
import datetime
import sqlite3
import os
import sys
import logging
import threading
import time

import numpy
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
WORKSPACE_DIR = 'workspace_not_a_dam'
PLANET_QUADS_DIR = os.path.join(WORKSPACE_DIR, 'planet_quads')
DAM_IMAGERY_DIR = os.path.join(WORKSPACE_DIR, 'dam_images')
GSW_DIR = os.path.join(WORKSPACE_DIR, 'gsw_tiles')
PLANET_STITCHED_IMAGERY_DIR = os.path.join(PLANET_QUADS_DIR, 'stiched_images')
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'not_a_dam.db')
DAM_STATUS_DB_PATH = os.path.join(WORKSPACE_DIR, 'dam_status.db')
N_WORKERS = -1
REPORTING_INTERVAL = 5.0
NOT_A_DAM_IMAGES_TO_CACHE = 10


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
                download_url(
                    src_url, surface_water_raster_path,
                    skip_if_target_exists=True)
                LOGGER.info('downloaded!')
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


def download_url(url, target_path, skip_if_target_exists=False):
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


if __name__ == '__main__':
    for dir_path in [PLANET_QUADS_DIR, DAM_IMAGERY_DIR, GSW_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    DB_CONN_THREAD_MAP = {}
    TASK_GRAPH = taskgraph.TaskGraph(
        WORKSPACE_DIR, N_WORKERS, reporting_interval=REPORTING_INTERVAL)
    IMAGE_CANDIDATE_QUEUE = queue.Queue()
    IMAGE_CANDIDATE_QUEUE.put(1)
    IMAGE_CANDIDATE_QUEUE.put('STOP')
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
