"""NatGeo flask data server."""
import glob
import os
import sys
import logging

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
        print(dirname)
        if not os.path.isdir(os.path.join(path, dirname)):
            continue
        image_list = []
        for file_path in glob.glob(os.path.join(path, dirname, '*.png')):
            image_list.append(file_path)
        directory_list.append((dirname, image_list))
    return directory_list


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
