"""Calculate dam diagonal bounds."""
import ast
import math
import sqlite3

import matplotlib.pyplot as plt
import numpy


def main():
    """Entry point."""
    database_path = r"C:\Users\rpsharp\Documents\dam_bounding_box_db.db"
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    cursor.execute('SELECT bounding_box_bounds FROM validation_table')
    bb_diag_len_list = []
    for payload in cursor:
        bounding_box_list = ast.literal_eval(payload[0])
        if bounding_box_list is not None:
            center_lat = (
                bounding_box_list[0]['lat'] + bounding_box_list[1]['lat'])
            lat_len_m, lng_len_m = len_of_deg_to_lat_lng_m(center_lat)
            bb_lat_m = abs(lat_len_m * (
                bounding_box_list[0]['lat'] - bounding_box_list[1]['lat']))
            bb_lng_m = abs(lng_len_m * (
                bounding_box_list[0]['lng'] - bounding_box_list[1]['lng']))
            bb_diag_len = math.sqrt(bb_lat_m**2+bb_lng_m**2)
            bb_diag_len_list.append(bb_diag_len)
    print(f'std: {numpy.std(bb_diag_len_list)}')
    print(f'mean: {numpy.mean(bb_diag_len_list)}')
    print(f'median: {numpy.median(bb_diag_len_list)}')
    print(f'max: {numpy.max(bb_diag_len_list)}')
    print(f'min: {numpy.min(bb_diag_len_list)}')

    n, bins, patches = plt.hist(bb_diag_len_list, bins=100)
    plt.show()


def len_of_deg_to_lat_lng_m(center_lat):
    """Calculate length of degree in m.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Parameters:
        len_in_deg (float): length in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.

    Returns:
        (lat len, lng len) in meters.

    """
    # Convert latitude to radians
    lat = center_lat * math.pi / 180
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118

    latlen = (
        m1 + m2*math.cos(2*lat) + m3*math.cos(4*lat) + m4*math.cos(6*lat))
    longlen = (
        p1*math.cos(lat) + p2*math.cos(3*lat) + p3*math.cos(5*lat))
    return (latlen, longlen)


if __name__ == '__main__':
    main()
