"""NatGeo flask data server."""
import sys
import logging

from flask import Flask


LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

APP = Flask(__name__, static_url_path='/static')

@APP.route('/')
def index():
    """Entry page."""
    try:
        return 'test'
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
