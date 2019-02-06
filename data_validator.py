"""Flask app to validata imagery and point locations."""
import datetime
import sqlite3
import collections
import re
import glob
import os
import sys
import logging

from osgeo import gdal
from flask import Flask
from flask import render_template

LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
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


@APP.route('/')
def index():
    """Entry page."""
    return "under construction"
    try:
        imagery_path = f'./{WORKSPACE_DIR}/sentinel_granules'
        return render_template(
            'index.html', image_list=search_images(imagery_path))
    except Exception as e:
        return str(e)


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
                * modified geometry (blob?) - to mark moved geometry?
        complete_token_path (str): path to file that will be created if



    Returns:
        None.
    """
    sql_create_projects_table = (
        """
        CREATE TABLE base_table (
            source_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            data_geom TEXT NOT NULL,
            key INTEGER NOT NULL PRIMARY KEY
        );
        CREATE UNIQUE INDEX IF NOT EXISTS base_table_index
        ON base_table (key);

        CREATE TABLE validation_table (
            modified_geom TEXT NOT NULL,
            key INTEGER NOT NULL,
            FOREIGN KEY (key) REFERENCES base_table(key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS validation_table_index
        ON validation_table (key);
        """)

    if os.path.exists(target_database_path):
        os.remove(target_database_path)

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
                    'INSERT INTO base_table VALUES (?, ?, ?, ?)',
                    (source_id, key_val, geom.ExportToWkt(), next_feature_id))
                next_feature_id += 1

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


if __name__ == '__main__':
    database_path = os.path.join(WORKSPACE_DIR, 'dam_point_db.db')
    complete_token_path = os.path.join(os.path.dirname(
        database_path), f'{os.path.basename(database_path)}_COMPLETE')
    build_base_validation_db(
        POINT_DAM_DATA_LIST, database_path, complete_token_path)
    APP.run(host='0.0.0.0', port=8080)
