"""
Load all data/graphs/*.pb (excluding graphs_all*), combine into a single graph with
universal stop and line indices, run point deduplication (reduce_points), then write
to data/combined_graphs/:

- graphs_all_structures.pb: lines and any other non-bulk structures (no stops/journeys).
- graphs_all_stops_<i>.pb: stops in chunks of MAX_PER_FILE (1M) to stay under protobuf limits.
- graphs_all_journeys_<i>.pb: journeys in chunks of MAX_PER_FILE (1M).

Uses flat arrays + offsets for journeys to avoid memory-heavy list-of-dicts.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import tqdm

from global_gtfs_graph import graph_pb2
from global_gtfs_graph.point_reduce import reduce_points

MAX_PER_FILE = 1_000_000
DEDUP_MAX_DISTANCE_M = 25.0
# In-place remap chunk size to avoid temporary allocation
_REMAP_CHUNK = 5_000_000


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def _load_feed_pb(pb_path: Path) -> graph_pb2.FeedGraph:
    pb = graph_pb2.FeedGraph()
    pb.ParseFromString(pb_path.read_bytes())
    return pb


def combine_graphs(base: Path | None = None) -> list[Path]:
    """
    Load all feed .pb files, merge into one graph with universal indices,
    run point deduplication, then write chunked protobufs.
    Returns list of paths written.
    """
    if base is None:
        base = _default_data_dir()
    graphs_dir = base / "graphs"
    if not graphs_dir.exists():
        raise SystemExit(f"Graphs directory not found: {graphs_dir}")

    out_dir = base / "combined_graphs"
    out_dir.mkdir(parents=True, exist_ok=True)

    pb_files = sorted(
        p for p in graphs_dir.glob("*.pb") if not p.name.startswith("graphs_all")
    )
    if not pb_files:
        raise SystemExit(f"No .pb graph files found under {graphs_dir}")

    # --- 1. Combine: flat arrays for stops; lines as list (small); journeys as flat + offsets ---
    lat_list: list[float] = []
    lon_list: list[float] = []
    stop_names: list[str] = []

    lines: list[dict] = []  # {line_id, name, color, type_id, agency_id} — small

    journey_line_ids: list[int] = []
    stops_flat_list: list[int] = []
    stops_offsets: list[int] = [0]
    times_flat_list: list[int] = []
    times_offsets: list[int] = [0]
    days_flat_list: list[int] = []
    days_offsets: list[int] = [0]

    next_stop_id = 0
    next_line_id = 0

    for pb_path in tqdm.tqdm(pb_files, desc="Loading feeds"):
        pb = _load_feed_pb(pb_path)
        feed_stop_to_global: dict[int, int] = {}
        feed_line_to_global: dict[int, int] = {}

        for s in pb.stops:
            gid = next_stop_id
            next_stop_id += 1
            feed_stop_to_global[s.stop_id] = gid
            lat_list.append(s.lat)
            lon_list.append(s.lon)
            stop_names.append(s.name or "")

        for ln in pb.lines:
            gid = next_line_id
            next_line_id += 1
            feed_line_to_global[ln.line_id] = gid
            lines.append({
                "line_id": gid,
                "name": ln.name or "",
                "color": ln.color or "",
                "type_id": int(ln.type_id),
                "agency_id": ln.agency_id or "",
            })

        for j in pb.journeys:
            if len(j.stops) < 2:
                continue
            global_stops = [feed_stop_to_global[sid] for sid in j.stops if sid in feed_stop_to_global]
            if len(global_stops) < 2:
                continue
            line_id = feed_line_to_global.get(j.line_id, 0)
            journey_line_ids.append(line_id)
            stops_flat_list.extend(global_stops)
            stops_offsets.append(len(stops_flat_list))
            times_flat_list.extend(int(t) for t in j.times_within_day)
            times_offsets.append(len(times_flat_list))
            days_flat_list.extend(int(d) for d in j.days)
            days_offsets.append(len(days_flat_list))

    n_stops = len(lat_list)
    n_lines = len(lines)
    n_journeys = len(journey_line_ids)
    print(f"Combined: {n_stops} stops, {n_lines} lines, {n_journeys} journeys")

    # Convert to numpy only what we need for dedup; drop list copies as we go
    lat = np.array(lat_list, dtype=np.float64)
    lon = np.array(lon_list, dtype=np.float64)
    del lat_list, lon_list

    # --- 2. Point deduplication ---
    if n_stops == 0:
        raise SystemExit("No stops to write.")
    with tqdm.tqdm(
        desc="Running point deduplication",
        total=1,
        unit="",
        bar_format="{desc}: {n_fmt}/{total_fmt} [{elapsed}]",
    ) as pbar:
        new_lat, new_lon, mapping = reduce_points(
            lat, lon, max_distance_m=DEDUP_MAX_DISTANCE_M
        )
        pbar.update(1)
    M = len(new_lat)
    print(f"After dedup: {M} representative stops (from {n_stops})")
    del lat, lon

    # Representative name = first original name mapping to that rep
    rep_names: list[str] = [""] * M
    for old_id in range(n_stops):
        r = int(mapping[old_id])
        if not rep_names[r]:
            rep_names[r] = stop_names[old_id]
    del stop_names

    # Flat journey arrays; remap stop ids in place
    line_ids_arr = np.array(journey_line_ids, dtype=np.int32)
    del journey_line_ids
    stops_flat = np.array(stops_flat_list, dtype=np.int32)
    del stops_flat_list
    stops_offsets_arr = np.array(stops_offsets, dtype=np.int64)
    del stops_offsets
    times_flat = np.array(times_flat_list, dtype=np.int32)
    del times_flat_list
    times_offsets_arr = np.array(times_offsets, dtype=np.int64)
    del times_offsets
    days_flat = np.array(days_flat_list, dtype=np.int32)
    del days_flat_list
    days_offsets_arr = np.array(days_offsets, dtype=np.int64)
    del days_offsets

    # Remap stop ids in place (chunked to avoid big temporary)
    mapping = np.asarray(mapping, dtype=np.int32)
    for start in range(0, len(stops_flat), _REMAP_CHUNK):
        end = min(start + _REMAP_CHUNK, len(stops_flat))
        stops_flat[start:end] = mapping[stops_flat[start:end]]

    # --- 3. Write chunked protobufs ---
    written: list[Path] = []

    # Structures: lines only
    structures_pb = graph_pb2.FeedGraph()
    for ln in lines:
        line = structures_pb.lines.add()
        line.line_id = ln["line_id"]
        line.name = ln["name"]
        line.color = ln["color"]
        line.type_id = ln["type_id"]
        line.agency_id = ln["agency_id"]
    structures_path = out_dir / "graphs_all_structures.pb"
    structures_path.write_bytes(structures_pb.SerializeToString())
    written.append(structures_path)

    # Stops: stream by chunk from arrays (no list of dicts)
    for chunk_start in range(0, M, MAX_PER_FILE):
        chunk_end = min(chunk_start + MAX_PER_FILE, M)
        pb = graph_pb2.FeedGraph()
        for i in range(chunk_start, chunk_end):
            stop = pb.stops.add()
            stop.stop_id = i
            stop.name = rep_names[i]
            stop.lat = float(new_lat[i])
            stop.lon = float(new_lon[i])
        idx = chunk_start // MAX_PER_FILE
        path = out_dir / f"graphs_all_stops_{idx}.pb"
        path.write_bytes(pb.SerializeToString())
        written.append(path)
    print(f"Wrote {((M - 1) // MAX_PER_FILE) + 1} stop chunk(s)")
    del new_lat, new_lon, rep_names

    # Journeys: stream by chunk from flat arrays
    for chunk_start in range(0, n_journeys, MAX_PER_FILE):
        chunk_end = min(chunk_start + MAX_PER_FILE, n_journeys)
        pb = graph_pb2.FeedGraph()
        for j in range(chunk_start, chunk_end):
            journey = pb.journeys.add()
            journey.line_id = int(line_ids_arr[j])
            s0, s1 = int(stops_offsets_arr[j]), int(stops_offsets_arr[j + 1])
            for s in stops_flat[s0:s1]:
                journey.stops.append(int(s))
            t0, t1 = int(times_offsets_arr[j]), int(times_offsets_arr[j + 1])
            for t in times_flat[t0:t1]:
                journey.times_within_day.append(int(t))
            d0, d1 = int(days_offsets_arr[j]), int(days_offsets_arr[j + 1])
            for d in days_flat[d0:d1]:
                journey.days.append(int(d))
        idx = chunk_start // MAX_PER_FILE
        path = out_dir / f"graphs_all_journeys_{idx}.pb"
        path.write_bytes(pb.SerializeToString())
        written.append(path)
    print(f"Wrote {((n_journeys - 1) // MAX_PER_FILE) + 1} journey chunk(s)")

    return written


def main() -> None:
    base = _default_data_dir()
    written = combine_graphs(base)
    print(f"Wrote {len(written)} file(s) under {base / 'combined_graphs'}.")


if __name__ == "__main__":
    main()
