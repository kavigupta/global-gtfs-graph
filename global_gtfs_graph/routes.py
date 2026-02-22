"""GTFS route type and valid route/trip filtering."""

import numpy as np

from . import gtfs_io


def is_route_type(route_type) -> bool:
    if not isinstance(route_type, (int, float, np.integer, np.floating)):
        return False
    if int(route_type) != route_type:
        return False
    route_type = int(route_type)
    if 0 <= route_type <= 12:
        return True
    if route_type // 100 in {1, 2, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17}:
        return True
    return False


def is_bus_or_ferry_route_type(route_type) -> bool:
    assert is_route_type(route_type), route_type
    if route_type in {3, 4}:
        return True
    if route_type // 100 in {
        2,
        7,
        10,
        11,
        12,
        13,
        15,
        17,
    }:
        return True
    return False


def valid_routes(gtfs, invalid_route_types) -> set:
    routes = gtfs_io.pull_file_from_gtfs(gtfs, "routes.txt")
    if routes is None:
        return set()
    valid_routes_set = set()
    for route_id, route_type in zip(routes["route_id"], routes["route_type"]):
        if route_type == route_type and not invalid_route_types(route_type):
            valid_routes_set.add(route_id)
    return valid_routes_set


def valid_trips(gtfs, invalid_route_types) -> set:
    valid_routes_set = valid_routes(gtfs, invalid_route_types)
    trips = gtfs_io.pull_file_from_gtfs(gtfs, "trips.txt")
    assert trips is not None, "trips.txt is missing"
    valid_trips_set = set()
    for trip_id, route_id in zip(trips["trip_id"], trips["route_id"]):
        if route_id in valid_routes_set:
            valid_trips_set.add(trip_id)
    return valid_trips_set
