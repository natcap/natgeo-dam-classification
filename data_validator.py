"""Flask app to validata imagery and point locations."""
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

grand_vector = gdal.OpenEx(
    r"workspace/GRanD_Version_1_1/GRanD_dams_v1_1.shp", gdal.OF_VECTOR)
grand_layer = grand_vector.GetLayer()
GRAND_ID_TO_NAME_MAP = {}
for grand_feature in grand_layer:
    GRAND_ID_TO_NAME_MAP[grand_feature.GetField('GRAND_ID')] = (
        grand_feature.GetField('DAM_NAME'))
grand_layer = None
grand_vector = None
WORKSPACE_DIR = 'workspace'

VALIDATION_DATABASE_PATH = os.path.join(WORKSPACE_DIR)


@APP.route('/')
def index():
    """Entry page."""
    try:
        imagery_path = f'./{WORKSPACE_DIR}/sentinel_granules'
        return render_template(
            'index.html', image_list=search_images(imagery_path))
    except Exception as e:
        return str(e)


def build_base_validation_db(point_shape_tuple_list, target_database_path):
    """Build the base database for validation.

    Parameters:
        point_shape_tuple_list (list): list of (vector_path, key) pairs
            that should be ingested into the target database. the
            `vector_path` component refers to a vector geometry on disk who
            has features that can be uniquely identified with the field

    """
    database_path = os.path.join(workspace_dir, 'ipbes_ndr_results.db')
    sql_create_projects_table = (
        """
        CREATE TABLE IF NOT EXISTS nutrient_export (
            ws_prefix_key TEXT NOT NULL,
            scenario_key TEXT NOT NULL,
            nutrient_export REAL NOT NULL,
            modified_load REAL NOT NULL,
            rural_pop_count REAL NOT NULL,
            average_runoff_coefficient REAL NOT NULL,
            ag_area REAL NOT NULL,
            total_ag_load REAL NOT NULL,
            grid_aggregation_pickle BLOB NOT NULL,
            PRIMARY KEY (ws_prefix_key, scenario_key)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ws_scenario_index
        ON nutrient_export (ws_prefix_key, scenario_key);
        CREATE INDEX IF NOT EXISTS ws_index
        ON nutrient_export (ws_prefix_key);

        CREATE TABLE IF NOT EXISTS geometry_table (
            ws_prefix_key TEXT NOT NULL,
            geometry_wgs84_wkb BLOB NOT NULL,
            region TEXT NOT NULL,
            country TEXT NOT NULL,
            watershed_area REAL NOT NULL,
            PRIMARY KEY (ws_prefix_key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS geometry_key_index
        ON geometry_table (ws_prefix_key);
        """)

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.executescript(sql_create_projects_table)


def search_images(path):
    """Build dict of images."""
    directory_list = []
    for dirname in os.listdir(path):
        if not os.path.isdir(os.path.join(path, dirname)):
            continue
        image_list = []
        grand_id_to_band_list = collections.defaultdict(list)
        for file_path in glob.glob(os.path.join(path, dirname, '*.png')):
            try:
                raster_band_id, grand_id = re.match(
                    r'.*_(.*)_grand_(.*)\.png', file_path).groups()
                if raster_band_id != 'TCI':
                    continue
                grand_id_to_band_list[grand_id].append(
                    (raster_band_id, file_path))
            except:
                LOGGER.exception('can\'t find a match on %s', file_path)
                continue
        for grand_id, image_list in grand_id_to_band_list.items():
            directory_list.append(
                (GRAND_ID_TO_NAME_MAP[int(grand_id)], image_list))
    return directory_list


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
