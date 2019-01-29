"""NatGeo flask data server."""
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

@APP.route('/')
def index():
    """Entry page."""
    try:
        path = './workspace/sentinel_granules'
        return render_template('index.html', image_list=search_images(path))
    except Exception as e:
        return str(e)


def search_images(path):
    """Build dict of images."""
    directory_list = []
    for dirname in os.listdir(path):
        if not os.path.isdir(os.path.join(path, dirname)):
            continue
        image_list = []
        grand_id = None
        for file_path in glob.glob(os.path.join(path, dirname, '*.png')):
            raster_band_id, grand_id = re.match(
                r'.*_(.*)_grand_(.*)\.png', file_path).groups()
            image_list.append((raster_band_id, file_path))

        directory_list.append((GRAND_ID_TO_NAME_MAP[int(grand_id)], image_list))
    return directory_list


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
