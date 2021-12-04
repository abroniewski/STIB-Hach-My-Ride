import csv
from contextlib import contextmanager
from math import radians, cos, sin, asin, sqrt
from typing import Iterable, Dict, Tuple, List


# ---------------------------------------------------------------
# ------------------------LatLon distance------------------------
# ---------------------------------------------------------------


def distance(lat1, lon1, lat2, lon2):
    # The math module contains a function named
    # radians which converts from degrees to radians.
    lon1 = radians(float(lon1))
    lon2 = radians(float(lon2))
    lat1 = radians(float(lat1))
    lat2 = radians(float(lat2))

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2

    c = 2 * asin(sqrt(a))

    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371

    # calculate the result
    return c * r


# ---------------------------------------------------------------
# ----------------------------CSVs IO----------------------------
# ---------------------------------------------------------------


def read_csv_list(path, as_dict=False):
    with open(path, 'r', encoding='utf8') as file:
        return list((csv.DictReader if as_dict else csv.reader)(file))


def read_csv_stream(path, as_dict=False, skip_first=True):
    with open(path, 'r', encoding='utf8') as file:
        reader = (csv.DictReader if as_dict else csv.reader)(file)
        if skip_first:
            next(reader)
        for line in reader:
            yield line


def get_csv_writer(path):
    file = open(path, 'w', encoding='utf8', newline='')
    return csv.writer(file), file


@contextmanager
def write_csv(path) -> csv.writer:
    file = open(path, 'w', encoding='utf8', newline='')
    try:
        yield csv.writer(file)
    finally:
        file.close()


# ---------------------------------------------------------------
# -----------------------Line manipulation-----------------------
# ---------------------------------------------------------------

def group_line_stops(line_stops: Iterable[str]) -> Dict[str, Tuple[List[List[str]], List[List[str]]]]:
    line_stops_map = {}
    for stop in line_stops:
        line_id = str(int(stop[0][0:-1]))
        direction = int(stop[1]) - 1
        if line_id not in line_stops_map:
            line_stops_map[line_id] = ([], [])
        line_stops_map[line_id][direction].append(stop)
    for line, directions in line_stops_map.items():
        for direction in directions:
            direction.sort(key=lambda stop: int(stop[-1]))
    return line_stops_map
