"""
Build per-feed graph files: stops (coords + name), journeys (line, local-time patterns between stop pairs),
and lines (name, color, type_id). Writes data/graphs/<feed_id>.pb (protobuf only).

Journey timings are expressed as:
- days: weekdays 0–6 (Python weekday, local calendar)
- timings: minutes from local midnight and duration in minutes.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Set

from . import calendar
from . import gtfs_io
from . import trips
from . import graph_pb2

DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


def _safe_float(x: Any):
    """
    Parse a coordinate; return None if invalid instead of coercing to 0.0.
    """
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN check
        return None
    return v


def _safe_feed_basename(feed_id: str) -> str:
    return re.sub(r"[^\w\-.]", "_", feed_id)


def _graph_pb_path(feed_id: str, base: Path = DEFAULT_DATA_DIR) -> Path:
    return base / "graphs" / f"{_safe_feed_basename(feed_id)}.pb"


def build_feed_graph(
    gtfs: dict,
    feed_id: str,
    base: Path = DEFAULT_DATA_DIR,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build the graph payload for one feed: stops, journeys, lines.
    Uses joined_calendar_dates (cached) to get service_id -> active dates. Journeys are
    represented as OD pairs (line_id, start_stop_id, end_stop_id) with per-day departure
    patterns encoded via days[] and timings[] in UTC.
    """
    # (1) Stops: every stop with normalized int stop_id (0..N-1)
    stops_df = gtfs_io.pull_file_from_gtfs(gtfs, "stops.txt")
    stop_list: List[Dict[str, Any]] = []
    gtfs_stop_id_to_int: Dict[str, int] = {}
    if stops_df is not None:
        next_stop_id = 0
        for _, row in stops_df.iterrows():
            lat = _safe_float(row.get("stop_lat"))
            lon = _safe_float(row.get("stop_lon"))
            if lat is None or lon is None:
                # Drop stops with invalid coordinates entirely; they won't appear in the graph.
                continue
            # Treat exact (0, 0) coordinates as invalid (common placeholder for missing location).
            if lat == 0.0 and lon == 0.0:
                continue
            name = str(row.get("stop_name", "") or "")
            gtfs_id = str(row["stop_id"])
            gtfs_stop_id_to_int[gtfs_id] = next_stop_id
            stop_list.append({
                "stop_id": next_stop_id,
                "name": name,
                "lat": lat,
                "lon": lon,
            })
            next_stop_id += 1

    # (2) Lines: normalized int line_id (0..N-1), name, color, type_id, agency_id
    routes_df = gtfs_io.pull_file_from_gtfs(gtfs, "routes.txt")
    line_list: List[Dict[str, Any]] = []
    route_id_to_line_int: Dict[str, int] = {}
    route_id_to_agency_id_raw: Dict[str, str] = {}
    if routes_df is not None:
        for idx, (_, row) in enumerate(routes_df.iterrows()):
            name = str(row.get("route_short_name") or row.get("route_long_name") or "")
            color = str(row.get("route_color", "") or "").strip()
            rt = row.get("route_type")
            type_id = int(rt) if rt is not None and str(rt) != "nan" else 0
            agency_id_raw = str(row.get("agency_id", "") or "")
            # Ensure `agency_id` is unique in our graph output so downstream exports (e.g. shapefiles)
            # don't collapse many feeds into the same empty-string agency.
            # TODO: Load real agency metadata from agency.txt (name/url/etc.) and preserve GTFS agency_id semantics.
            agency_id = f"{feed_id}:{agency_id_raw or 'default'}"
            route_id = str(row["route_id"])
            route_id_to_line_int[route_id] = idx
            route_id_to_agency_id_raw[route_id] = agency_id_raw
            line_list.append({
                "line_id": idx,
                "name": name,
                "color": color,
                "type_id": type_id,
                "agency_id": agency_id,
            })

    # agency_id -> timezone (from agency.txt; default UTC)
    agency_df = gtfs_io.pull_file_from_gtfs(gtfs, "agency.txt")
    agency_id_to_tz: Dict[str, str] = {}
    if agency_df is not None:
        for _, row in agency_df.iterrows():
            aid = str(row.get("agency_id", "") or "")
            tz = str(row.get("agency_timezone", "UTC") or "UTC").strip()
            agency_id_to_tz[aid] = tz if tz else "UTC"

    # trip_id -> line_id (int), trip_id -> agency_id (for timezone)
    trips_df = gtfs_io.pull_file_from_gtfs(gtfs, "trips.txt")
    trip_to_line_int: Dict[str, int] = {}
    trip_to_tz: Dict[str, str] = {}
    if trips_df is not None:
        for _, row in trips_df.iterrows():
            rid = str(row["route_id"])
            tid = str(row["trip_id"])
            if rid in route_id_to_line_int:
                trip_to_line_int[tid] = route_id_to_line_int[rid]
            aid = route_id_to_agency_id_raw.get(rid, "")
            trip_to_tz[tid] = agency_id_to_tz.get(aid, "UTC")

    # service_id -> set of weekdays (0=Monday, ..., 6=Sunday)
    joined = calendar.joined_calendar_dates(gtfs, feed_id=feed_id, base=base)
    service_weekdays: Dict[str, set] = {}
    for sid, dates in joined.items():
        service_weekdays[str(sid)] = {d.weekday() for d in dates}

    # (3) Journeys:
    #     One Journey per (line_id, start_stop_id, end_stop_id) with:
    #     - days: list of local weekdays 0=Monday .. 6=Sunday when that segment runs
    #     - timings: (start_min local, duration minutes) within the day.
    #     When a stop was dropped earlier we bridge over it (A-B-C-E).
    journey_patterns: Dict[tuple[int, int, int], tuple[Set[int], List[tuple[int, int]]]] = {}
    for trip_id, service_id, stop_times in trips.compute_trip_stop_times(gtfs):
        if not stop_times:
            continue
        line_int = trip_to_line_int.get(str(trip_id))
        if line_int is None:
            continue
        weekdays = service_weekdays.get(str(service_id))
        if not weekdays:
            continue

        valid_stops: List[tuple[int, int]] = []
        for sid, mm in stop_times:
            sid_int = gtfs_stop_id_to_int.get(str(sid))
            if sid_int is None:
                continue
            valid_stops.append((sid_int, int(mm)))
        if len(valid_stops) < 2:
            continue

        for (start_int, start_mm), (end_int, end_mm) in zip(valid_stops, valid_stops[1:]):
            key = (line_int, start_int, end_int)
            if key not in journey_patterns:
                journey_patterns[key] = (set(), [])
            days_set, timings_list = journey_patterns[key]
            for wd in weekdays:
                # Local calendar weekday and times; we intentionally ignore DST offsets here
                # and treat all days as having the same local schedule shape.
                days_set.add(int(wd))
                duration = end_mm - start_mm
                timings_list.append((start_mm, duration))

    journey_list: List[Dict[str, Any]] = []
    for (line_int, start_int, end_int), (days_set, timings_list) in journey_patterns.items():
        # Weekdays 0-6, dedupe and sort timings
        days_sorted = sorted(days_set)
        timings_deduped = sorted(set(timings_list))
        journey_list.append(
            {
                "line_id": line_int,
                "start_stop_id": start_int,
                "end_stop_id": end_int,
                "days": days_sorted,
                "timings": [{"start_min": sm, "duration": dur} for sm, dur in timings_deduped],
            }
        )

    # Compact stops: keep only those that are referenced by at least one journey, and
    # renumber stop_id to be dense 0..N-1. Journeys are updated to use the new IDs.
    used_stop_ids = set()
    for j in journey_list:
        used_stop_ids.add(int(j["start_stop_id"]))
        used_stop_ids.add(int(j["end_stop_id"]))

    if used_stop_ids:
        old_to_new: Dict[int, int] = {}
        new_stops: List[Dict[str, Any]] = []
        next_stop_id = 0
        for s in stop_list:
            sid = int(s["stop_id"])
            if sid not in used_stop_ids:
                continue
            old_to_new[sid] = next_stop_id
            s_new = dict(s)
            s_new["stop_id"] = next_stop_id
            new_stops.append(s_new)
            next_stop_id += 1

        stop_list = new_stops
        for j in journey_list:
            j["start_stop_id"] = old_to_new[int(j["start_stop_id"])]
            j["end_stop_id"] = old_to_new[int(j["end_stop_id"])]
    else:
        # No journeys: drop all stops as well.
        stop_list = []

    return {"stops": stop_list, "lines": line_list, "journeys": journey_list}


def _payload_to_feed_graph_pb(payload: Dict[str, List[Dict[str, Any]]]) -> bytes:
    """Serialize graph payload dict to FeedGraph protobuf bytes."""
    pb = graph_pb2.FeedGraph()
    for s in payload.get("stops", []):
        stop = pb.stops.add()
        stop.stop_id = int(s.get("stop_id", 0))
        stop.name = s.get("name", "")
        stop.lat = s.get("lat", 0.0)
        stop.lon = s.get("lon", 0.0)
    for ln in payload.get("lines", []):
        line = pb.lines.add()
        line.line_id = int(ln.get("line_id", 0))
        line.name = ln.get("name", "")
        line.color = ln.get("color", "")
        line.type_id = ln.get("type_id", 0)
        line.agency_id = ln.get("agency_id", "")
    for j in payload.get("journeys", []):
        journey = pb.journeys.add()
        journey.line_id = int(j.get("line_id", 0))
        journey.start_stop_id = int(j.get("start_stop_id", 0))
        journey.end_stop_id = int(j.get("end_stop_id", 0))
        for d in j.get("days", []):
            journey.days.append(int(d))
        for t in j.get("timings", []):
            timing = journey.timings.add()
            timing.start_min = int(t.get("start_min", 0))
            timing.duration = int(t.get("duration", 0))
    return pb.SerializeToString()


def write_feed_graph(
    gtfs: dict,
    feed_id: str,
    base: Path = DEFAULT_DATA_DIR,
) -> Path:
    """Build the graph for this feed and write data/graphs/<feed_id>.pb. Returns path written."""
    print(f"Building graph for feed {feed_id}")
    payload = build_feed_graph(gtfs, feed_id=feed_id, base=base)
    out_pb = _graph_pb_path(feed_id, base)
    out_pb.parent.mkdir(parents=True, exist_ok=True)
    out_pb.write_bytes(_payload_to_feed_graph_pb(payload))
    return out_pb
