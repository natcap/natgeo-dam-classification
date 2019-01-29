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

APP = Flask(__name__, static_url_path='/workspace/sentinel_granules/')

@APP.route('/')
def index():
    """Entry page."""
    try:
        return '<img src="/workspace/sentinel_granules/L1C_T10UEB_A012108_20171016T193021/T10UEB_20171016T192401_B02_grand_19.png">'
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
