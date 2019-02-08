"""Flask app to validata imagery and point locations."""
import pathlib
import io
import zipfile
import json
import datetime
import sqlite3
import os
import sys
import logging

import shapely.wkt
import shapely.geometry
from osgeo import gdal
from flask import Flask
import flask

import sentinel_data_fetch


LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

APP = Flask(__name__, static_url_path='', static_folder='')

POINT_DAM_DATA_LIST = [
    ('GRAND', "workspace/GRanD_Version_1_1/GRanD_dams_v1_1.shp", 'GRAND_ID'),
]
WORKSPACE_DIR = 'workspace'

VALIDATION_DATABASE_PATH = os.path.join(WORKSPACE_DIR)
DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'dam_point_db.db')


@APP.route('/')
def entry_point():
    """This handles the root GET."""
    return process_point('0')


@APP.route('/unvalidated')
def get_unvalidated_point():
    """Get a point that has not been validated."""
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT key '
            'FROM base_table '
            'WHERE key not in (SELECT key from validation_table)'
            'LIMIT 1')
        unvalidated_point_id = cursor.fetchone()[0]
        return process_point(unvalidated_point_id)


@APP.route('/summary')
def summary_page():
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT count(*) from base_table')
        n_points = int(cursor.fetchone()[0])
        cursor.execute(
            'SELECT source_id, source_key, key, data_geom '
            'FROM base_table '
            'WHERE key in (SELECT key from validation_table)')
        validated_dam_key_tuple_list = []
        for payload in cursor:
            source_id, source_key, key, data_geom = payload
            point_id = f'{source_id}({source_key})'
            sample_point = shapely.wkt.loads(payload[3])
            image_path = sentinel_data_fetch.get_bounding_box_imagery(
                sample_point, point_id, WORKSPACE_DIR)
            LOGGER.debug(image_path)
            validated_dam_key_tuple_list.append(
                (point_id, key, image_path))
            break

    return flask.render_template(
        'summary.html', **{
            'n_points': n_points,
            'validated_dam_key_tuple_list': validated_dam_key_tuple_list,
        })

@APP.route('/download-all-zip')
def request_zip():
    """Request zip of all imagery."""
    try:
        image_path_key_list = []
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT count(*) from base_table')
            cursor.execute(
                'SELECT source_id, source_key, key, data_geom '
                'FROM base_table')
            LOGGER.debug("cursor result: %s", cursor)
            for payload in cursor:
                source_id, source_key, key, data_geom = payload
                point_id = f'{source_id}({source_key})'
                sample_point = shapely.wkt.loads(payload[3])
                LOGGER.debug('fetching imagery for %s', sample_point)
                image_path = sentinel_data_fetch.get_bounding_box_imagery(
                    sample_point, point_id, WORKSPACE_DIR)
                LOGGER.debug('path: %s', image_path)
                if image_path is None:
                    continue
                ext = os.path.splitext(image_path)[1]
                image_path_key_list.append(
                    (image_path, f'{source_id}_{source_key}{ext}'))

        data = io.BytesIO()
        with zipfile.ZipFile(data, mode='w') as z:
            for image_path, arc_path in image_path_key_list:
                z.write(image_path, arc_path)

        data.seek(0)
        return flask.send_file(
            data,
            mimetype='application/zip',
            as_attachment=True,
            attachment_filename='image_dump.zip'
        )
    except Exception as e:
        LOGGER.exception('something bad happened')
        return str(e)

@APP.route('/<point_id>')
def process_point(point_id):
    """Entry page."""
    LOGGER.debug(point_id)
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT source_id, source_key, data_geom '
                'from base_table WHERE key = ?', (point_id,))
            source_id, source_key, geometry_wkt = cursor.fetchone()
            base_point_geom = shapely.wkt.loads(geometry_wkt)
            base_point_id = f'{source_id}({source_key})'

            cursor.execute(
                'SELECT validated_geom '
                'from validation_table WHERE key = ?', (point_id,))
            validated_geometry_wkt = cursor.fetchone()
            if validated_geometry_wkt is not None:
                validated_geometry = shapely.wkt.loads(
                    validated_geometry_wkt[0])
            else:
                validated_geometry = base_point_geom

        return flask.render_template(
            'validation.html', **{
                'point_id': point_id,
                'base_point_id': base_point_id,
                'base_point_geom': base_point_geom,
                'validated_point_geom': validated_geometry,
            })
    except Exception as e:
        return str(e)


@APP.route('/markermove', methods=['POST'])
def move_marker():
    """Push event on a marker."""
    try:
        LOGGER.debug('got a post')
        payload = json.loads(flask.request.data.decode('utf-8'))
        LOGGER.debug(payload)
        point_geom = shapely.geometry.Point(payload['lng'], payload['lat'])
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO validation_table VALUES (?, ?)',
                (point_geom.wkt, payload['point_id']))
        LOGGER.debug('move marker')
        return 'good'
    except:
        LOGGER.exception("big error")
        return 'error'


def build_base_validation_db(
        point_shape_tuple_list, target_database_path, complete_token_path):
    """Build the base database for validation.

    Parameters:
        point_shape_tuple_list (list): list of (vector_path, key) pairs
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
                * validated_geom text (wkt of moved point)
        complete_token_path (str): path to file that will be created if



    Returns:
        None.
    """
    sql_create_projects_table = (
        """
        CREATE TABLE IF NOT EXISTS base_table (
            source_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            data_geom TEXT NOT NULL,
            key INTEGER NOT NULL PRIMARY KEY
        );
        CREATE UNIQUE INDEX IF NOT EXISTS base_table_index
        ON base_table (key);

        CREATE TABLE IF NOT EXISTS validation_table (
            validated_geom TEXT NOT NULL,
            key INTEGER NOT NULL PRIMARY KEY,
            FOREIGN KEY (key) REFERENCES base_table(key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS validation_table_index
        ON validation_table (key);
        """)

    with sqlite3.connect(target_database_path) as conn:
        cursor = conn.cursor()
        cursor.executescript(sql_create_projects_table)

        next_feature_id = 0
        for source_id, vector_path, primary_key_id in point_shape_tuple_list:
            vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
            layer = vector.GetLayer()
            for feature in layer:
                geom = feature.GetGeometryRef()
                key_val = feature.GetField(primary_key_id)
                cursor.execute(
                    'INSERT OR IGNORE INTO base_table VALUES (?, ?, ?, ?)',
                    (source_id, key_val, geom.ExportToWkt(), next_feature_id))
                next_feature_id += 1

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def init():
    """Initialize system."""


if __name__ == '__main__':
    sentinel_data_fetch.build_index()

    complete_token_path = os.path.join(os.path.dirname(
        DATABASE_PATH), f'{os.path.basename(DATABASE_PATH)}_COMPLETE')
    build_base_validation_db(
        POINT_DAM_DATA_LIST, DATABASE_PATH, complete_token_path)
    APP.run(host='0.0.0.0', port=8080)
