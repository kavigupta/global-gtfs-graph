"""
Read the combined graph (data/combined_graphs/) and write subgraphs for each 1x1 degree
cell. Each subgraph includes:

- All stops in the cell or within 6 km of the cell border (2h * 3 km walking buffer).
- All stops reachable within 2 hours by journeys from those seed stops.

Uses a journey graph (min duration between consecutive stops) and Dijkstra from seed
stops with a 2-hour time budget.

Output: data/combined_graphs/cells/<lat>_<lon>.pb

Usage (from repo root):
    python scripts/split_combined_graph_by_cell.py
"""

from __future__ import annotations

import heapq
import math
from pathlib import Path

import tqdm

from global_gtfs_graph import graph_pb2

# 6 km buffer from cell border (2h * 3 km/h walking)
BORDER_BUFFER_KM = 6.0
# Max travel time from seed stops (minutes)
MAX_TRAVEL_MINUTES = 120
# Approx km per degree lat at mid-latitudes
KM_PER_DEG_LAT = 111.32


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def _lon_km_per_deg_at_lat(lat_deg: float) -> float:
    """Approx km per degree longitude at latitude."""
    return KM_PER_DEG_LAT * max(math.cos(math.radians(lat_deg)), 1e-6)


def load_combined_graph(base: Path) -> tuple[list, list, list]:
    """
    Load structures, all stops, all journeys from combined_graphs.
    Returns (stops, lines, journeys) where stops/journeys are lists of dicts.
    """
    combined_dir = base / "combined_graphs"
    if not combined_dir.exists():
        raise SystemExit(f"Combined graphs directory not found: {combined_dir}")

    # Lines
    structures_path = combined_dir / "graphs_all_structures.pb"
    if not structures_path.exists():
        raise SystemExit(f"Structures not found: {structures_path}")
    pb = graph_pb2.FeedGraph()
    pb.ParseFromString(structures_path.read_bytes())
    lines = [
        {
            "line_id": ln.line_id,
            "name": ln.name,
            "color": ln.color,
            "type_id": int(ln.type_id),
            "agency_id": ln.agency_id,
        }
        for ln in pb.lines
    ]

    # Stops (all chunks)
    stops: list[dict] = []
    for p in sorted(combined_dir.glob("graphs_all_stops_*.pb")):
        pb = graph_pb2.FeedGraph()
        pb.ParseFromString(p.read_bytes())
        for s in pb.stops:
            stops.append(
                {
                    "stop_id": s.stop_id,
                    "name": s.name,
                    "lat": s.lat,
                    "lon": s.lon,
                }
            )

    # Journeys (all chunks)
    journeys: list[dict] = []
    for p in sorted(combined_dir.glob("graphs_all_journeys_*.pb")):
        pb = graph_pb2.FeedGraph()
        pb.ParseFromString(p.read_bytes())
        for j in pb.journeys:
            if len(j.stops) < 2:
                continue
            journeys.append(
                {
                    "line_id": j.line_id,
                    "stops": list(j.stops),
                    "times_within_day": list(j.times_within_day),
                    "days": list(j.days),
                }
            )

    return stops, lines, journeys


def build_journey_graph(journeys: list[dict]) -> dict[int, list[tuple[int, int]]]:
    """
    Build adjacency: stop_id -> [(neighbor_id, duration_minutes), ...].
    For each consecutive pair in a journey, keep minimum duration.
    """
    # (from_id, to_id) -> min_duration
    edges: dict[tuple[int, int], int] = {}
    for j in journeys:
        stops = j["stops"]
        times = j["times_within_day"]
        if len(times) != len(stops):
            continue
        for i in range(len(stops) - 1):
            a, b = stops[i], stops[i + 1]
            t_a, t_b = times[i], times[i + 1]
            duration = (t_b - t_a + 24 * 60) % (24 * 60)  # handle overnight
            key = (a, b)
            if key not in edges or edges[key] > duration:
                edges[key] = duration

    # Convert to adjacency list
    adj: dict[int, list[tuple[int, int]]] = {}
    for (a, b), d in edges.items():
        adj.setdefault(a, []).append((b, d))
    return adj


def dijkstra_reachable(
    adj: dict[int, list[tuple[int, int]]],
    seed_stops: set[int],
    max_minutes: int,
) -> set[int]:
    """
    Return all stops reachable from seed_stops within max_minutes.
    Uses Dijkstra: min distance from any seed to each stop.
    """
    dist: dict[int, int] = {s: 0 for s in seed_stops}
    heap = [(0, s) for s in seed_stops]
    heapq.heapify(heap)

    while heap:
        d, u = heapq.heappop(heap)
        if d > dist[u]:
            continue
        for v, duration in adj.get(u, []):
            new_d = d + duration
            if new_d > max_minutes:
                continue
            if new_d < dist.get(v, max_minutes + 1):
                dist[v] = new_d
                heapq.heappush(heap, (new_d, v))

    return set(dist.keys())


def cells_with_seeds(
    stops: list[dict], buffer_km: float
) -> dict[tuple[int, int], set[int]]:
    """
    For each 1x1 cell (lat_floor, lon_floor), return set of stop_ids in the seed region
    (cell + buffer). Seed region: [lat_floor - buffer, lat_floor + 1 + buffer) x [lon_floor - buffer, lon_floor + 1 + buffer).
    """
    buffer_lat_deg = buffer_km / KM_PER_DEG_LAT
    cell_to_stops: dict[tuple[int, int], set[int]] = {}
    for s in stops:
        lat, lon = s["lat"], s["lon"]
        stop_id = s["stop_id"]
        # Candidate i cells (latitude) that could include this point in their expanded region.
        i_lo = int(math.floor(lat - buffer_lat_deg))
        i_hi = int(math.floor(lat + buffer_lat_deg))
        for i in range(i_lo, i_hi + 1):
            # Longitude buffer must expand with latitude: degrees get smaller near poles.
            lat_more_polar = max(
                [lat - buffer_lat_deg, i, lat + buffer_lat_deg], key=abs
            )
            buffer_lon_deg = buffer_km / _lon_km_per_deg_at_lat(lat_more_polar)

            j_lo = int(math.floor(lon - buffer_lon_deg))
            j_hi = int(math.floor(lon + buffer_lon_deg))
            for j in range(j_lo, j_hi + 1):
                if (
                    i - buffer_lat_deg <= lat < i + 1 + buffer_lat_deg
                    and j - buffer_lon_deg <= lon < j + 1 + buffer_lon_deg
                ):
                    key = (i, j)
                    if key not in cell_to_stops:
                        cell_to_stops[key] = set()
                    cell_to_stops[key].add(stop_id)
    return cell_to_stops


def run(base: Path | None = None) -> list[Path]:
    """
    Load combined graph, build journey graph, for each 1x1 cell with seeds run
    Dijkstra, filter subgraph, write to data/combined_graphs/cells/<lat>_<lon>.pb.
    Returns list of paths written.
    """
    if base is None:
        base = _default_data_dir()

    print("Loading combined graph...")
    stops, lines, journeys = load_combined_graph(base)
    print(f"Loaded {len(stops)} stops, {len(lines)} lines, {len(journeys)} journeys")

    stop_by_id = {s["stop_id"]: s for s in stops}
    line_by_id = {ln["line_id"]: ln for ln in lines}

    print("Building journey graph (min duration between stops)...")
    adj = build_journey_graph(journeys)

    buffer_lat_deg = BORDER_BUFFER_KM / KM_PER_DEG_LAT
    print(
        f"Finding cells with seeds (buffer={BORDER_BUFFER_KM} km; "
        f"lat≈{buffer_lat_deg:.4f} deg, lon padded by 1/cos(lat))..."
    )
    cell_to_seeds = cells_with_seeds(stops, BORDER_BUFFER_KM)
    cells = sorted(cell_to_seeds.keys())
    print(f"Processing {len(cells)} cells...")

    out_dir = base / "combined_graphs" / "cells"
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for lat_floor, lon_floor in tqdm.tqdm(cells, desc="Cells"):
        seed_stops = cell_to_seeds[(lat_floor, lon_floor)]
        reachable = dijkstra_reachable(adj, seed_stops, MAX_TRAVEL_MINUTES)

        # Filter journeys: only those whose stops are all in reachable
        sub_journeys = []
        for j in journeys:
            if all(s in reachable for s in j["stops"]):
                sub_journeys.append(j)

        # Lines used by sub_journeys
        used_line_ids = {j["line_id"] for j in sub_journeys}
        sub_lines = [
            line_by_id[lid] for lid in sorted(used_line_ids) if lid in line_by_id
        ]

        # Renumber stops and lines for this subgraph (0..N-1)
        old_stop_ids = sorted(reachable)
        old_to_new_stop = {old: i for i, old in enumerate(old_stop_ids)}
        old_line_ids = sorted(used_line_ids)
        old_to_new_line = {old: i for i, old in enumerate(old_line_ids)}

        pb = graph_pb2.FeedGraph()
        for old_sid in old_stop_ids:
            s = stop_by_id[old_sid]
            stop = pb.stops.add()
            stop.stop_id = old_to_new_stop[old_sid]
            stop.name = s["name"]
            stop.lat = s["lat"]
            stop.lon = s["lon"]
        for ln in sub_lines:
            line = pb.lines.add()
            line.line_id = old_to_new_line[ln["line_id"]]
            line.name = ln["name"]
            line.color = ln["color"]
            line.type_id = ln["type_id"]
            line.agency_id = ln["agency_id"]
        for j in sub_journeys:
            journey = pb.journeys.add()
            journey.line_id = old_to_new_line[j["line_id"]]
            for s in j["stops"]:
                journey.stops.append(old_to_new_stop[s])
            for t in j["times_within_day"]:
                journey.times_within_day.append(t)
            for d in j["days"]:
                journey.days.append(d)

        path = out_dir / f"{lat_floor}_{lon_floor}.pb"
        path.write_bytes(pb.SerializeToString())
        written.append(path)

    return written


def main() -> None:
    base = _default_data_dir()
    written = run(base)
    print(
        f"Wrote {len(written)} cell subgraph(s) to {base / 'combined_graphs' / 'cells'}."
    )


if __name__ == "__main__":
    main()
