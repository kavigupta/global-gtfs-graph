"""GTFS trip and stop_times processing.

Times are standardized as minutes from midnight within a day. For the multigraph,
edges use minutes from week start (Monday 00:00): use minutes_from_week_start().
"""

import datetime
from collections import defaultdict
from typing import List, Tuple

from . import gtfs_io

MINUTES_PER_DAY = 24 * 60 


def parse_time(time_str: str) -> datetime.timedelta:
    """
    Parses a time string in HH:MM:SS format and returns timedelta since midnight.
    Handles times that exceed 24 hours (e.g. 25:00:00).
    """
    assert isinstance(time_str, str), f"Expected string, got {type(time_str)}"
    h, m, s = map(int, time_str.split(":"))
    return datetime.timedelta(hours=h, minutes=m, seconds=s)


def time_to_minutes_from_midnight(time_str: str) -> int:
    """Convert GTFS time string (HH:MM:SS) to minutes since midnight"""
    td = parse_time(time_str)
    total_seconds = td.total_seconds()
    return int(total_seconds // 60)


def minutes_from_week_start(day_of_week: int, minutes_from_midnight: int) -> int:
    """
    Standardized time: minutes from week start (Monday 00:00).
    day_of_week: Python weekday (Monday=0, Tuesday=1, ..., Sunday=6).
    Monday 00:00 -> 0, Tuesday 00:00 -> 1440, ..., Sunday 23:59 -> 10079.
    """
    return day_of_week * MINUTES_PER_DAY + minutes_from_midnight


def compute_trip_stop_times(gtfs) -> List[Tuple[str, str, List[Tuple[str, int]]]]:
    """
    Computes stop times for each trip. No remapping: raw stop_id and service_id.

    Returns a list of (trip_id, service_id, [(stop_id, minutes_from_midnight), ...]).
    minutes_from_midnight is 0–1439 (or more if GTFS time > 24:00). Use
    minutes_from_week_start(day_of_week, minutes_from_midnight) with the service
    calendar to get standardized edge times (minutes from Monday 00:00).
    """
    stop_times_df = gtfs_io.pull_file_from_gtfs(gtfs, "stop_times.txt")
    if stop_times_df is None:
        return []

    trip_stop_times: dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for trip_id, arrival_time, departure_time, stop_id in zip(
        stop_times_df["trip_id"],
        stop_times_df["arrival_time"],
        stop_times_df["departure_time"],
        stop_times_df["stop_id"],
    ):
        start_min = time_to_minutes_from_midnight(arrival_time)
        end_min = time_to_minutes_from_midnight(departure_time)
        minutes_from_midnight = (start_min + end_min) // 2
        trip_stop_times[trip_id].append((minutes_from_midnight, stop_id))

    trip_id_to_service_id: dict[str, str] = {}
    trips_df = gtfs_io.pull_file_from_gtfs(gtfs, "trips.txt")
    if trips_df is not None:
        for trip_id, service_id in zip(trips_df["trip_id"], trips_df["service_id"]):
            trip_id_to_service_id[trip_id] = service_id

    result: List[Tuple[str, str, List[Tuple[str, int]]]] = []
    for trip_id, stop_times_list in trip_stop_times.items():
        if trip_id not in trip_id_to_service_id:
            continue
        service_id = trip_id_to_service_id[trip_id]
        # Sort by time along the trip
        stop_times_list.sort(key=lambda x: x[0])
        result.append(
            (
                trip_id,
                service_id,
                [(stop_id, min_from_midnight) for min_from_midnight, stop_id in stop_times_list],
            )
        )
    return result
