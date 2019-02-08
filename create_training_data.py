"""Script to iterate through GRaND database and extract clips."""
import time
import zipfile
import os
import sys
import logging

import reproduce
import taskgraph
from sentinelsat.sentinel import SentinelAPI
from sentinelsat.sentinel import SentinelAPIError
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import shapely.wkb
import shapely.wkt
import shapely.geometry

# set a 1GB limit for the cache
gdal.SetCacheMax(2**30)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)

DAM_POINT_VECTOR_BUCKET_ID_PATH = (
    'natgeo-data-bucket',
    'GRanD_Version_1_1_md5_9ad04293d056cd35abceb8a15b953fb8.zip')
IAM_TOKEN_PATH = 'ecoshard-202992-key.json'
WORKSPACE_DIR = 'workspace'
GRAND_VECTOR_PATH = os.path.join(
    WORKSPACE_DIR, 'GRanD_Version_1_1', 'GRanD_dams_v1_1.shp')
COVERAGE_VECTOR_PATH = os.path.join(WORKSPACE_DIR, 'coverage_vector.gpkg')


def main():
    """Entry point."""
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1, 30.0)
    target_dam_vector_path = WORKSPACE_DIR
    dam_fetch_unzip_task = fetch_and_unzip(
        task_graph, DAM_POINT_VECTOR_BUCKET_ID_PATH, IAM_TOKEN_PATH,
        target_dam_vector_path)

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    gpkg_driver = ogr.GetDriverByName('GPKG')
    if os.path.exists(COVERAGE_VECTOR_PATH):
        gpkg_driver.DeleteDataSource(COVERAGE_VECTOR_PATH)
    coverage_vector = gpkg_driver.CreateDataSource(COVERAGE_VECTOR_PATH)
    coverage_layer = coverage_vector.CreateLayer(
        COVERAGE_VECTOR_PATH, wgs84_srs)
    coverage_layer.CreateField(
        ogr.FieldDefn('grand_id', ogr.OFTInteger))
    coverage_layer.CreateField(
        ogr.FieldDefn('coverage_or_boundingbox', ogr.OFTString))
    coverage_layer.CreateField(
        ogr.FieldDefn('sentinel_id', ogr.OFTString))
    coverage_layer.SyncToDisk()
    coverage_layer = None
    coverage_vector = None

    dam_fetch_unzip_task.join()
    grand_vector = gdal.OpenEx(GRAND_VECTOR_PATH, gdal.OF_VECTOR)
    grand_layer = grand_vector.GetLayer()
    for grand_point_feature in grand_layer:
        grand_point_geom = grand_point_feature.GetGeometryRef()
        grand_point_shapely = shapely.wkb.loads(grand_point_geom.ExportToWkb())

        # 300m in degrees at equator
        grand_bb = shapely.geometry.box(
            *grand_point_shapely.buffer(0.002713115198871344).bounds)

        # create a bounding box polygon and dump to vector so we can examine
        # it while the downloads process
        coverage_vector = gdal.OpenEx(
            COVERAGE_VECTOR_PATH, gdal.OF_VECTOR | gdal.GA_Update)
        coverage_layer = coverage_vector.GetLayer()
        coverage_defn = coverage_layer.GetLayerDefn()
        grand_bb_feature = ogr.Feature(coverage_defn)
        grand_bb_feature.SetGeometry(ogr.CreateGeometryFromWkt(grand_bb.wkt))
        grand_bb_feature.SetField('coverage_or_boundingbox', 'boundingbox')
        grand_bb_feature.SetField(
            'grand_id', grand_point_feature.GetField('GRAND_ID'))
        coverage_layer.CreateFeature(grand_bb_feature)
        coverage_layer.SyncToDisk()
        coverage_layer = None
        coverage_vector = None
        grand_bb_feature = None

        /tiles/[UTM_ZONE]/[LATITUDE_BAND]/[GRID_SQUARE]/[GRANULE_ID]/...

        """
        previous_area = grand_bb.area
        while grand_bb.area > 0:
            while True:
                # sometimes the service goes down, keep trying every 5 mins
                # if this happens
                try:
                    sentinel_products = sentinel_api.query(
                        grand_bb.wkt,
                        platformname='Sentinel-2',
                        cloudcoverpercentage=(0, 0),
                        area_relation='Contains',
                        order_by='-cloudcoverpercentage,ingestiondate',
                        limit=1)
                    break
                except SentinelAPIError:
                    LOGGER.exception(
                        "Query error, waiting 5 mins and trying again.")
                    time.sleep(60*5)
            if not sentinel_products:
                LOGGER.debug('no hits on %s', grand_bb)
                break

            sentinel_key, product = next(iter(sentinel_products.items()))
            LOGGER.debug('downloading %s', sentinel_key)
            download_task = task_graph.add_task(
                func=sentinel_api.download,
                args=(sentinel_key, WORKSPACE_DIR),
                ignore_directories=False,  # target is a directory
                target_path_list=[os.path.join(
                    WORKSPACE_DIR, f"{product['identifier']}.zip")],
                task_name=f"download {product['identifier']}")
            # explicitly join so we don't overwhelm the Sentinel API
            download_task.join()

            # create a coverage polygon and dump to vector so we can examine
            # while the downloads process
            coverage_vector = gdal.OpenEx(
                COVERAGE_VECTOR_PATH, gdal.OF_VECTOR | gdal.GA_Update)
            coverage_layer = coverage_vector.GetLayer()
            coverage_defn = coverage_layer.GetLayerDefn()
            coverage_feature = ogr.Feature(coverage_defn)
            coverage_footprint = shapely.wkt.loads(product['footprint'])
            coverage_feature.SetGeometry(
                ogr.CreateGeometryFromWkt(product['footprint']))
            coverage_feature.SetField('coverage_or_boundingbox', 'coverage')
            coverage_feature.SetField('sentinel_id', sentinel_key)
            coverage_feature.SetField(
                'grand_id', grand_point_feature.GetField('GRAND_ID'))
            coverage_layer.CreateFeature(coverage_feature)
            coverage_layer.SyncToDisk()
            coverage_layer = None
            coverage_vector = None

            # subtract off the coverage
            grand_bb = grand_bb.difference(coverage_footprint)
            LOGGER.debug('post-download grand_bb area: %f', grand_bb.area)
            if previous_area == grand_bb.area:
                break
            else:
                previous_area = grand_bb.area
            """
        grand_point_feature = None

    task_graph.close()
    task_graph.join()


def fetch_and_unzip(
        task_graph, bucket_id_path_tuple, iam_token_path, target_zipfile_dir):
    """Schedule a bucket fetch and file unzip.

    Parameters:
        task_graph (TaskGraph): taskgraph object to schedule download and
            unzip.
        bucket_id_path_tuple (tuple): a google bucket id / path tuple for the
            expected bucket and the path inside it to the zipfile.
        iam_token_path (str): path to IAM token to access `bucket_id`.
        target_zipfile_dir (str): desired directory to copy `bucket_path` and
            unzip its contents. This directory will also have an
            `os.path.basename(bucket_id_path_tuple[1]).UNZIPPED` file when the
            unzip task is successful.

    Returns:
        Task object that when complete will have downloaded and unzipped
            the files specified above.

    """
    zipfile_basename = os.path.basename(bucket_id_path_tuple[1])
    expected_zip_path = os.path.join(target_zipfile_dir, zipfile_basename)
    fetch_task = task_graph.add_task(
        func=reproduce.utils.google_bucket_fetch_and_validate,
        args=(
            bucket_id_path_tuple[0], bucket_id_path_tuple[1], iam_token_path,
            expected_zip_path),
        target_path_list=[expected_zip_path],
        task_name=f'''fetch {zipfile_basename}''')

    unzip_file_token = f'{expected_zip_path}.UNZIPPED'
    unzip_task = task_graph.add_task(
        func=unzip_file,
        args=(
            expected_zip_path, target_zipfile_dir, unzip_file_token),
        target_path_list=[unzip_file_token],
        dependent_task_list=[fetch_task],
        task_name=f'unzip {zipfile_basename}')
    return unzip_task


def unzip_file(zipfile_path, target_dir, touchfile_path):
    """Unzip contents of `zipfile_path`.

    Parameters:
        zipfile_path (string): path to a zipped file.
        target_dir (string): path to extract zip file to.
        touchfile_path (string): path to a file to create if unzipping is
            successful.

    Returns:
        None.

    """
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    with open(touchfile_path, 'w') as touchfile:
        touchfile.write(f'unzipped {zipfile_path}')


if __name__ == '__main__':
    main()
