"""GTFS calendar and service date handling."""

import datetime
from bisect import bisect_right
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import tqdm.auto as tqdm
from permacache import permacache

from . import feeds
from . import gtfs_io


def parse_date(date_str: str, default: datetime.date = None) -> Optional[datetime.date]:
    if date_str != date_str:  # NaN check
        if default is not None:
            return default
        return None
    if isinstance(date_str, (float, int, np.float32, np.float64, np.int64)):
        if date_str > 20991231:
            return default
        date_str = str(int(date_str))
    try:
        return datetime.datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None


def process_calendar_row(row) -> Set[datetime.date]:
    start_date = parse_date(row["start_date"])
    end_date = parse_date(row["end_date"], default=datetime.date(2099, 12, 31))
    if start_date is None or end_date is None:
        return set()
    active_days = set()
    delta = datetime.timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        if (
            (weekday == 0 and row.monday == 1)
            or (weekday == 1 and row.tuesday == 1)
            or (weekday == 2 and row.wednesday == 1)
            or (weekday == 3 and row.thursday == 1)
            or (weekday == 4 and row.friday == 1)
            or (weekday == 5 and row.saturday == 1)
            or (weekday == 6 and row.sunday == 1)
        ):
            active_days.add(current_date)
        current_date += delta
    return active_days


def calendar_dates_to_calendar_txt(exceptions_table) -> pd.DataFrame:
    """
    Converts calendar_dates.txt to calendar.txt format.
    This only applies in situations where calendar.txt is missing.
    """
    exceptions_table = exceptions_table[exceptions_table.exception_type == 1].copy()
    exceptions_table.date = exceptions_table.date.apply(
        lambda x: datetime.datetime.strptime(str(x), "%Y%m%d").date()
    )
    num_days = (exceptions_table.date.max() - exceptions_table.date.min()).days + 1
    exceptions_table["day_of_week"] = exceptions_table.date.apply(lambda x: x.weekday())
    pivot = pd.pivot_table(
        exceptions_table,
        index="service_id",
        columns="day_of_week",
        aggfunc="count",
        values="date",
        fill_value=0,
    )
    return pivot / num_days * 7


def joined_calendar_dates(gtfs) -> Dict[str, Set[datetime.date]]:
    """
    Returns a map of feed IDs to their active dates.
    """
    calendar = gtfs_io.pull_file_from_gtfs(gtfs, "calendar.txt")
    calendar_dates = gtfs_io.pull_file_from_gtfs(gtfs, "calendar_dates.txt")

    active_dates: Dict[str, Set[datetime.date]] = defaultdict(set)

    if calendar is not None:
        calendar = calendar[
            (calendar.monday == 1)
            | (calendar.tuesday == 1)
            | (calendar.wednesday == 1)
            | (calendar.thursday == 1)
            | (calendar.friday == 1)
            | (calendar.saturday == 1)
            | (calendar.sunday == 1)
        ]
        for _, row in calendar.iterrows():
            active_dates[row["service_id"]] = process_calendar_row(row)

    if calendar_dates is not None:
        calendar_dates.date = calendar_dates.date.apply(parse_date)
        for exc, date, service_id in zip(
            calendar_dates["exception_type"],
            calendar_dates["date"],
            calendar_dates["service_id"],
        ):
            if date is None:
                continue
            if exc == 1:
                active_dates[service_id].add(date)
            elif exc == 2:
                active_dates[service_id].discard(date)
            else:
                raise ValueError(f"Unknown exception_type: {exc}")
    assert active_dates, "No active dates found in GTFS data."
    return active_dates


def date_range_from_joined_calendar(
    joined_calendar: Dict[str, Set[datetime.date]],
) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    all_dates = set()
    for dates in joined_calendar.values():
        all_dates.update(dates)
    if not all_dates:
        return None, None
    return min(all_dates), max(all_dates)


def reverse_joined_calendar(
    joined_calendar: Dict[str, Set[datetime.date]],
) -> Dict[datetime.date, Set[str]]:
    date_to_service_ids: Dict[datetime.date, Set[str]] = defaultdict(set)
    for service_id, dates in joined_calendar.items():
        for date in dates:
            date_to_service_ids[date].add(service_id)
    return date_to_service_ids


def most_covered_period_of_length(
    start_ends: List[Tuple[datetime.date, datetime.date]], length: datetime.timedelta
) -> Tuple[int, Optional[datetime.date], Optional[datetime.date]]:
    """
    Given a list of (start_date, end_date) tuples, finds the longest continuous period.
    """
    all_dates = sorted({x for start, end in start_ends for x in (start, end)})
    date_to_idx = {date: idx for idx, date in enumerate(all_dates)}
    non_cumulative_counts = [0] * (len(all_dates))
    for start, end in start_ends:
        start_idx = date_to_idx[start]
        end_idx = date_to_idx[end]
        non_cumulative_counts[start_idx] += 1
        non_cumulative_counts[end_idx] -= 1
    cumulative_counts = np.cumsum(non_cumulative_counts)
    assert cumulative_counts[-1] == 0
    cumulative_counts = cumulative_counts[:-1]
    best_count = 0
    best_start = None
    for start_idx, start_date in enumerate(all_dates):
        end_date = start_date + length
        end_idx = bisect_right(all_dates, end_date) - 1
        count = cumulative_counts[start_idx:end_idx].max() if end_idx > start_idx else 0
        if count > best_count:
            best_count = count
            best_start = start_date
    if best_start is None:
        return 0, None, None
    return best_count, best_start, best_start + length


def index_from_start(num_days: int, index: int, modulo_7_offset: int) -> int:
    location = index + modulo_7_offset
    while location >= num_days:
        location -= num_days // 7 * 7
    assert 0 <= location
    return location


def duplicate_and_shift_calendar(
    start: datetime.date,
    end: datetime.date,
    start_common: datetime.date,
    end_common: datetime.date,
) -> Optional[List[datetime.date]]:
    """
    Returns the usable subrange of the original calendar that can be
    used to map the common period.
    """
    num_days_common = (end_common - start_common).days + 1
    num_days_original = (end - start).days + 1
    if num_days_original < 7:
        return None
    num_days_to_pull = min(num_days_original, num_days_common)
    assert num_days_common % 7 == 0, "Common period must be a multiple of 7 days."
    if start > start_common:
        day_offset = (start_common - start).days % 7
        return [
            start
            + datetime.timedelta(days=index_from_start(num_days_to_pull, i, day_offset))
            for i in range(num_days_common)
        ]
    elif end < end_common:
        day_offset = (end_common - end).days % 7
        return [
            end
            - datetime.timedelta(
                days=index_from_start(num_days_to_pull, i, (-day_offset) % 7)
            )
            for i in range(num_days_common - 1, -1, -1)
        ]
    else:
        return [
            start_common + datetime.timedelta(days=i) for i in range(num_days_common)
        ]


@permacache("urbanstats/osm/trains/standardize_calendars_3", multiprocess_safe=True)
def standardize_calendars():
    dates = {}
    for res in feeds.all_gtfs_info():
        r = res["gtfs_result"]()
        if r["status"] == "failure":
            continue
        dates[res["feed"]["id"]] = joined_calendar_dates(r["content"])
    time_extrema = {k: date_range_from_joined_calendar(x) for k, x in dates.items()}
    time_extrema = {k: x for k, x in time_extrema.items() if x[0] is not None}
    _, start_common, end_common = most_covered_period_of_length(
        list(time_extrema.values()), datetime.timedelta(days=27)
    )
    date_remap = {
        k: duplicate_and_shift_calendar(start, end, start_common, end_common)
        for k, (start, end) in time_extrema.items()
    }
    date_remap = {k: v for k, v in date_remap.items() if v is not None}
    services = {}
    for k in tqdm.tqdm(date_remap):
        reversed_calendar = reverse_joined_calendar(dates[k])
        services[k] = [reversed_calendar[x] for x in date_remap[k]]
    day_to_standardized_service_ids, agency_mappings = standardize_service_ids(
        services
    )
    return day_to_standardized_service_ids, agency_mappings, start_common, end_common


def standardize_service_ids(
    services: Dict[str, List[Set[str]]],
) -> Tuple[List[Set[int]], Dict[str, Dict[str, int]]]:
    """
    Given a dict mapping agency id to list of sets of service IDs (one set per day),
    standardizes the service IDs across agencies.
    """
    standardized_service_id = 0
    agency_mappings: Dict[str, Dict[str, int]] = {}
    day_to_standardized_service_ids: List[Set[int]] = []

    for agency_id, agency_services in services.items():
        service_id_mapping: Dict[str, int] = {}
        for day_services in agency_services:
            standardized_ids_for_day: Set[int] = set()
            for service_id in sorted(day_services, key=repr):
                if service_id not in service_id_mapping:
                    service_id_mapping[service_id] = standardized_service_id
                    standardized_service_id += 1
                standardized_ids_for_day.add(service_id_mapping[service_id])
            day_to_standardized_service_ids.append(standardized_ids_for_day)
        agency_mappings[agency_id] = service_id_mapping

    return day_to_standardized_service_ids, agency_mappings
