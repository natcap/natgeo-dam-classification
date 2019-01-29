"""NatGeo flask data server."""
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

APP = Flask(__name__)

@APP.route('/')
def index():
    """Entry page."""
    try:
        path = './workspace/sentinel_granules'
        return render_template('index.html', tree=make_tree(path))
    except Exception as e:
        return str(e)


def make_tree(path):
    """Make a directory tree struct."""
    tree = dict(name=os.path.basename(path), children=[])
    try:
        lst = os.listdir(path)
    except OSError:
        pass  #ignore errors
    else:
        for name in lst:
            fn = os.path.join(path, name)
            if os.path.isdir(fn):
                tree['children'].append(make_tree(fn))
            else:
                tree['children'].append(dict(name=name))
    return tree

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=8080)
