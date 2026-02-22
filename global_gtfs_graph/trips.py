"""GTFS trip and stop_times processing."""

import datetime
from collections import defaultdict
from typing import Dict, List, Tuple

from . import gtfs_io


def parse_time(time_str: str) -> datetime.timedelta:
    """
    Parses a time string in HH:MM:SS format and returns timedelta object since midnight.
    Handles times that exceed 24 hours.
    """
    assert isinstance(time_str, str), f"Expected string, got {type(time_str)}"
    h, m, s = map(int, time_str.split(":"))
    return datetime.timedelta(hours=h, minutes=m, seconds=s)


def compute_trip_stop_times(
    gtfs, remap_services, remap_stops
) -> List[Tuple[str, List[Tuple[str, datetime.time]]]]:
    """
    Computes the stop times for each trip in the GTFS data.
    Returns a list of (service_id, List of (stop_id, time)) tuples.
    """
    stop_times = gtfs_io.pull_file_from_gtfs(gtfs, "stop_times.txt")
    if stop_times is None:
        return []

    trip_stop_times: Dict[str, List[Tuple[datetime.time, str]]] = defaultdict(list)

    for trip_id, arrival_time, departure_time, stop_id in zip(
        stop_times["trip_id"],
        stop_times["arrival_time"],
        stop_times["departure_time"],
        stop_times["stop_id"],
    ):
        time = (parse_time(arrival_time) + parse_time(departure_time)) / 2
        # Convert timedelta to time (midnight + delta)
        midnight = datetime.datetime(1, 1, 1)
        time_obj = (midnight + time).time()
        trip_stop_times[trip_id].append((time_obj, remap_stops[stop_id]))

    trip_id_to_service_id = {}
    trips = gtfs_io.pull_file_from_gtfs(gtfs, "trips.txt")
    if trips is not None:
        for trip_id, service_id in zip(trips["trip_id"], trips["service_id"]):
            trip_id_to_service_id[trip_id] = service_id

    # Convert to list of (service_id, [(stop_id, time), ...])
    result = []
    for trip_id, stop_times_list in trip_stop_times.items():
        if trip_id not in trip_id_to_service_id:
            continue
        service_id = trip_id_to_service_id[trip_id]
        if service_id not in remap_services:
            continue
        result.append(
            (
                remap_services[service_id],
                [(stop_id, t) for t, stop_id in stop_times_list],
            )
        )
    return result
