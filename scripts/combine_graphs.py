"""
Compute connected components of agencies based on spatial proximity and write
the result to data/connected_components.json (no combined graph .pb files).

Two agencies are connected if their nearest stops are less than 20 km apart (Haversine).
Agency = (feed_id, agency_id).

Usage (from repo root):
    python scripts/combine_graphs.py
"""

import json
import math
import random
from pathlib import Path
from collections import deque
import numpy as np
import networkx as nx
import tqdm

from global_gtfs_graph import geo
from global_gtfs_graph import graph_pb2

NEAREST_STOP_KM = 20.0
# Max points per agency used for connectivity (sample if more) to keep component step fast
MAX_POINTS_PER_AGENCY_FOR_CONNECTIVITY = 200


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def _min_distance_km(stops_a: list[tuple[float, float]], stops_b: list[tuple[float, float]]) -> float:
    """Minimum distance in km between any point in stops_a and any in stops_b."""
    if not stops_a or not stops_b:
        return float("inf")
    best = float("inf")
    for (la, loa) in stops_a:
        for (lb, lob) in stops_b:
            d = geo.haversine_km(la, loa, lb, lob)
            if d < best:
                best = d
    return best


def _bbox_expand_deg(km: float) -> float:
    """Approximate degrees for km at mid-latitudes (for bbox filter)."""
    return (km / 111.0)  # 1 deg ~ 111 km


def _load_feed_data(pb_path: Path) -> tuple[graph_pb2.FeedGraph, dict[str, str], dict[tuple[str, str], list[tuple[float, float]]]]:
    """Load pb, return (pb, line_id_to_agency_id, agency_key -> list of (lat, lon)).

    We no longer rely on precomputed edges in the protobuf; instead we derive a set of
    representative points per (feed_id, agency_id) from journey start/end stops. This
    is sufficient for spatial connectivity between agencies.
    """
    pb = graph_pb2.FeedGraph()
    pb.ParseFromString(pb_path.read_bytes())
    feed_id = pb_path.stem

    stop_coords: dict[int, tuple[float, float]] = {s.stop_id: (s.lat, s.lon) for s in pb.stops}
    line_to_agency: dict[str, str] = {}
    for ln in pb.lines:
        line_to_agency[ln.line_id] = ln.agency_id or ""

    agency_stops: dict[tuple[str, str], list[tuple[float, float]]] = {}
    for j in pb.journeys:
        if not j.stops:
            continue
        agency_id = line_to_agency.get(j.line_id, "")
        key = (feed_id, agency_id)
        if key not in agency_stops:
            agency_stops[key] = []
        for sid in (j.stops[0], j.stops[-1]):
            if sid in stop_coords:
                agency_stops[key].append(stop_coords[sid])

    # Deduplicate points per agency (same stop can appear in many edges)
    for key in agency_stops:
        seen: set[tuple[float, float]] = set()
        unique = []
        for p in agency_stops[key]:
            if p not in seen:
                seen.add(p)
                unique.append(p)
        agency_stops[key] = unique

    return pb, line_to_agency, agency_stops


def _connected_components(
    nodes: list[tuple[str, str]],
    agency_stops: dict[tuple[str, str], list[tuple[float, float]]],
    max_km: float,
) -> list[list[tuple[str, str]]]:
    agency_stops = {k: np.array(v) for k, v in agency_stops.items()}
    mean_each = {k: np.mean(v, axis=0) for k, v in agency_stops.items()}
    radii_each = {k: geo.haversine_km(mean_each[k][0], mean_each[k][1], agency_stops[k][:, 0], agency_stops[k][:, 1]).max() for k in agency_stops}
    keys = sorted(agency_stops)
    radii_each = np.array([radii_each[k] for k in keys])
    mean_each = np.array([mean_each[k] for k in keys])
    distance_matrix = geo.haversine_km(mean_each[:, 0][None], mean_each[:, 1][None], mean_each[:, 0][:, None], mean_each[:, 1][:, None])
    minimum_distance_matrix = distance_matrix - (radii_each[:, None] + radii_each[None, :])
    edges = []
    for i, j in tqdm.tqdm(list(zip(*np.where(minimum_distance_matrix < max_km)))):
        if i >= j:
            continue
        i, j = keys[i], keys[j]
        a, b = agency_stops[i], agency_stops[j]
        dist = geo.haversine_km(a[:, 0][None], a[:, 1][None], b[:, 0][:, None], b[:, 1][:, None]).min()
        # print(i, j, dist)
        if dist < max_km:
            edges.append((i, j, dist))
    G = nx.Graph()
    G.add_edges_from([(x, y) for x, y, _ in edges])
    components = list(nx.connected_components(G))
    return components


def combine_graphs(base: Path | None = None) -> Path:
    """
    Load all data/graphs/*.pb, partition agencies by connected components (nearest stops < 20 km),
    and write data/connected_components.json with the list of feeds per component.
    Returns the path written.
    """
    if base is None:
        base = _default_data_dir()
    graphs_dir = base / "graphs"
    if not graphs_dir.exists():
        raise SystemExit(f"Graphs directory not found: {graphs_dir}")

    pb_files = sorted(p for p in graphs_dir.glob("*.pb") if not p.name.startswith("graphs_all"))
    if not pb_files:
        raise SystemExit(f"No .pb graph files found under {graphs_dir}")

    # Load all feeds and collect agency -> stops
    feed_pbs: dict[str, graph_pb2.FeedGraph] = {}
    feed_line_to_agency: dict[str, dict[int, str]] = {}
    all_agency_stops: dict[tuple[str, str], list[tuple[float, float]]] = {}

    for pb_path in pb_files:
        feed_id = pb_path.stem
        pb, line_to_agency, agency_stops = _load_feed_data(pb_path)
        feed_pbs[feed_id] = pb
        feed_line_to_agency[feed_id] = line_to_agency
        for key, pts in agency_stops.items():
            all_agency_stops[key] = all_agency_stops.get(key, []) + pts

    # Deduplicate points per agency again (across feeds we only had per-feed; same agency key can appear in one feed only)
    nodes = list(all_agency_stops.keys())
    # Sample points per agency so component computation stays tractable
    sampled: dict[tuple[str, str], list[tuple[float, float]]] = {}
    for key, pts in all_agency_stops.items():
        if len(pts) <= MAX_POINTS_PER_AGENCY_FOR_CONNECTIVITY:
            sampled[key] = pts
        else:
            sampled[key] = random.sample(pts, MAX_POINTS_PER_AGENCY_FOR_CONNECTIVITY)
    print(f"Computing connected components for {len(nodes)} agencies (edge if nearest stops < {NEAREST_STOP_KM} km)...")
    components = _connected_components(nodes, sampled, NEAREST_STOP_KM)
    print(f"Found {len(components)} component(s).")

    # For each component, collect the set of feeds it contains.
    components_feeds: list[list[str]] = []
    for comp in components:
        feeds_in_comp = sorted({feed_id for feed_id, _ in comp})
        components_feeds.append(feeds_in_comp)

    out_path = base / "connected_components.json"
    out_path.write_text(
        json.dumps(
            {
                "nearest_stop_km": NEAREST_STOP_KM,
                "components": components_feeds,
            },
            indent=2,
        )
    )
    return out_path


def main() -> None:
    base = _default_data_dir()
    out_path = combine_graphs(base)
    print(
        f"Wrote connected components (by agencies linked if nearest stops < {NEAREST_STOP_KM} km) to {out_path}"
    )


if __name__ == "__main__":
    main()
