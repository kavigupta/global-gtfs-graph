"""GTFS trip and stop_times processing.

Times are standardized as minutes from midnight within a day. For the multigraph,
edges use minutes from week start (Monday 00:00): use minutes_from_week_start().
With timezone support, local times are converted to UTC and wrapped into [0, 10079].
"""

import datetime
from collections import defaultdict
from typing import List, Tuple
from zoneinfo import ZoneInfo

from . import gtfs_io

MINUTES_PER_DAY = 24 * 60
MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY  # 10080


def parse_time(time_str: str) -> datetime.timedelta:
    """
    Parses a time string in HH:MM:SS format and returns timedelta since midnight.
    Handles times that exceed 24 hours (e.g. 25:00:00).
    """
    if not isinstance(time_str, str):
        time_str = str(time_str).strip()
    if not time_str or time_str.lower() in ("nan", "nat", ""):
        raise ValueError(f"Invalid time: {time_str!r}")
    h, m, s = map(int, time_str.split(":"))
    return datetime.timedelta(hours=h, minutes=m, seconds=s)


def time_to_minutes_from_midnight(time_str: str) -> int:
    """Convert GTFS time string (HH:MM:SS) to minutes since midnight. Raises on invalid/NaN."""
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


def local_minutes_to_week_minutes_utc(
    date: datetime.date,
    minutes_from_midnight_local: int,
    timezone_name: str,
) -> int:
    """
    Convert (date, minutes from midnight) in the given timezone to minutes from week start (Monday 00:00) in UTC.
    Result is wrapped into [0, MINUTES_PER_WEEK) so times that span the week boundary are normalized.
    """
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:
        tz = datetime.timezone.utc
    local_dt = datetime.datetime(
        date.year, date.month, date.day, tzinfo=tz
    ) + datetime.timedelta(minutes=minutes_from_midnight_local)
    utc_dt = local_dt.astimezone(datetime.timezone.utc)
    weekday = utc_dt.weekday()
    minutes_from_midnight_utc = utc_dt.hour * 60 + utc_dt.minute
    return (minutes_from_week_start(weekday, minutes_from_midnight_utc)) % MINUTES_PER_WEEK


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
        try:
            start_min = time_to_minutes_from_midnight(arrival_time)
            end_min = time_to_minutes_from_midnight(departure_time)
        except (ValueError, TypeError):
            continue
        minutes_from_midnight = (start_min + end_min) // 2
        trip_stop_times[trip_id].append((minutes_from_midnight, str(stop_id)))

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
