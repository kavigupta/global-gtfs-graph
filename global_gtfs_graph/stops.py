"""GTFS stops extraction, deduplication, and spatial clustering."""

from collections import defaultdict
from typing import Dict, List, Set, Tuple

import numpy as np
import pandas as pd
import tqdm.auto as tqdm
from permacache import permacache

from . import feeds
from . import gtfs_io
from . import routes


def parse_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def collect_stops(gtfs):
    stops = gtfs_io.pull_file_from_gtfs(gtfs, "stops.txt")
    if stops is None:
        return None
    if "stop_lat" not in stops or "stop_lon" not in stops:
        raise ValueError("Missing stop_lat or stop_lon in stops.txt")
    return stops.stop_lat.tolist(), stops.stop_lon.tolist()


def stops_covered_by_valid_trips(gtfs, invalid_route_types) -> Set[str]:
    valid_trips_set = routes.valid_trips(gtfs, invalid_route_types)
    stop_times = gtfs_io.pull_file_from_gtfs(gtfs, "stop_times.txt")
    assert stop_times is not None, "stop_times.txt is missing"
    covered_stops = set()
    for trip_id, stop_id in zip(stop_times["trip_id"], stop_times["stop_id"]):
        if trip_id in valid_trips_set:
            covered_stops.add(stop_id)
    return covered_stops


def clean_up_parents(stops: pd.DataFrame) -> pd.DataFrame:
    if "parent_station" not in stops.columns:
        return stops
    assert stops.stop_id.apply(
        lambda x: str(x).strip() != ""
    ).all(), "Empty stop_id found"
    stops["parent_station"] = stops["parent_station"].apply(
        lambda x: x.strip() if isinstance(x, str) and x.strip() != "" else np.nan
    )
    return stops


def compute_stop_graph_within_radius(
    radius_in_km: float, lon: np.ndarray, lat: np.ndarray
) -> List[Tuple[int, int]]:
    from urbanstats.geometry.ellipse import Ellipse

    indices = np.argsort(lat)
    lats_in_order = lat[indices]
    lons_in_order = lon[indices]
    edges = []
    for i in tqdm.trange(len(lat), desc="Computing stop graph", delay=10):
        ellipse = Ellipse(radius_in_km, lats_in_order[i], lons_in_order[i])
        start_i = np.searchsorted(
            lats_in_order,
            lats_in_order[i] - ellipse.lat_radius,
            side="right",
        )
        end_i = i
        if end_i == start_i:
            continue
        lat_selected, lon_selected = (
            lats_in_order[start_i:end_i],
            lons_in_order[start_i:end_i],
        )
        ellipse_mask = ((lat_selected - lats_in_order[i]) / ellipse.lat_radius) ** 2 + (
            (lon_selected - lons_in_order[i]) / ellipse.lon_radius
        ) ** 2 < 1
        for j in np.where(ellipse_mask)[0]:
            edges.append((indices[i], indices[start_i + j]))
    return edges


def connected_components(
    edges: List[Tuple[int, int]], num_nodes: int
) -> List[Set[int]]:
    parent = list(range(num_nodes))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rootX = find(x)
        rootY = find(y)
        if rootX != rootY:
            parent[rootY] = rootX

    for u, v in edges:
        union(u, v)

    components: Dict[int, Set[int]] = defaultdict(set)
    for i in range(num_nodes):
        components[find(i)].add(i)

    return list(components.values())


def shatter_clusters_by_distance(
    lons: pd.Series,
    lats: pd.Series,
    clustered_indices: List[List[int]],
    max_distance_m,
) -> List[List[int]]:
    lons, lats = np.array(lons), np.array(lats)
    shattered = []
    for cluster in clustered_indices:
        edges = compute_stop_graph_within_radius(
            max_distance_m / 1000, lons[cluster], lats[cluster]
        )
        components = connected_components(edges, len(lons[cluster]))
        components = [x for x in components if len(x) > 1]
        components = [[cluster[i] for i in comp] for comp in components]
        shattered.extend(components)
    return shattered


def deduplicate_stops(
    stops: pd.DataFrame, remap: Dict[str, str]
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    stops = stops.reset_index(drop=True)
    clustered_indices = defaultdict(list)
    for idx, stop_name in zip(stops.index, stops["stop_name"]):
        clustered_indices[stop_name].append(idx)
    clustered_indices = [sorted(v) for k, v in clustered_indices.items() if len(v) > 1]
    clustered_indices = shatter_clusters_by_distance(
        stops.stop_lon, stops.stop_lat, clustered_indices, max_distance_m=250
    )
    new_rows = []
    remove_indices = []
    for indices in clustered_indices:
        cluster = stops.loc[indices]
        mean_lat = cluster["stop_lat"].mean()
        mean_lon = cluster["stop_lon"].mean()
        row_exemplar = cluster.loc[indices[0]].copy()
        row_exemplar["stop_lat"] = mean_lat
        row_exemplar["stop_lon"] = mean_lon
        new_rows.append(row_exemplar)
        exemplar_id = row_exemplar["stop_id"]
        for idx in indices:
            if stops.at[idx, "stop_id"] != exemplar_id:
                remap[stops.at[idx, "stop_id"]] = exemplar_id
        remove_indices.extend(indices)
    stops = stops.drop(index=remove_indices)
    stops = pd.concat([stops, pd.DataFrame(new_rows)], ignore_index=True)
    return stops, remap


def pull_stops_for_gtfs(
    gtfs, invalid_route_types
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    stops = gtfs_io.pull_file_from_gtfs(gtfs, "stops.txt")
    stops = clean_up_parents(stops)
    assert stops is not None, "stops.txt is missing"
    covered_stops = stops_covered_by_valid_trips(gtfs, invalid_route_types)
    referenced_stops = stops[stops["stop_id"].isin(covered_stops)].copy()
    remap = {}
    if "parent_station" in referenced_stops.columns:
        mask = referenced_stops["parent_station"].apply(
            lambda p: p == p and p in referenced_stops["stop_id"].values
        )
        with_parent, without_parent = (
            referenced_stops.loc[mask].copy(),
            referenced_stops.loc[~mask].copy(),
        )
        original, parent = with_parent["stop_id"], with_parent["parent_station"]
        remap = dict(zip(original, parent))
        referenced_stops = pd.concat(
            [
                without_parent,
                stops[stops.stop_id.isin(parent.unique())],
            ]
        )
    referenced_stops, remap = deduplicate_stops(referenced_stops, remap)
    return referenced_stops, remap


def pull_stops_for_gtfs_arrays(
    gtfs, invalid_route_types, start_idx
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, Dict[str, int]]:
    stops, remap_parent = pull_stops_for_gtfs(gtfs, invalid_route_types)
    names, lats, lons = [], [], []
    remap = {}
    for stop_id, stop_name, stop_lat, stop_lon in zip(
        stops["stop_id"], stops["stop_name"], stops["stop_lat"], stops["stop_lon"]
    ):
        stop_lat, stop_lon = parse_float(stop_lat), parse_float(stop_lon)
        if (
            stop_lat is None
            or stop_lon is None
            or np.isnan(stop_lat)
            or np.isnan(stop_lon)
        ):
            continue
        names.append(stop_name)
        lats.append(stop_lat)
        lons.append(stop_lon)
        remap[stop_id] = start_idx + len(names) - 1
    for stop_id, parent_station in remap_parent.items():
        remap[stop_id] = remap[parent_station]
    return (
        np.array(names),
        np.array(lats),
        np.array(lons),
        remap,
    )


@permacache("urbanstats/osm/trains/all_stops_6")
def all_stops():
    lats, lons = [], []
    ids = []
    for gtfs_info in feeds.all_gtfs_info():
        res = gtfs_info["gtfs_result"]()
        if res["status"] != "success":
            continue
        stops = collect_stops(res["content"])
        if stops is None:
            print(
                "Missing stops.txt in feed",
                gtfs_info["feed"]["id"],
                "available feeds:",
                res["content"].keys(),
            )
            continue
        lat_list, lon_list = stops
        lats.extend(lat_list)
        lons.extend(lon_list)
        ids += [gtfs_info["feed"]["id"]] * len(lat_list)
    return pd.DataFrame(dict(lat=lats, lon=lons, feed_id=ids))


def standardized_stops():
    names, feed_ids, lats, lons = [], [], [], []
    remaps = {}
    start_idx = 0
    for gtfs_info in feeds.all_gtfs_info():
        r = gtfs_info["gtfs_result"]()
        if r["status"] != "success":
            continue
        (
            names_array,
            lats_array,
            lons_array,
            remap,
        ) = pull_stops_for_gtfs_arrays(
            r["content"], routes.is_bus_or_ferry_route_type, start_idx
        )
        names.append(names_array)
        feed_ids.extend([gtfs_info["feed"]["id"]] * len(names_array))
        lats.append(lats_array)
        lons.append(lons_array)
        for k, v in remap.items():
            remaps[gtfs_info["feed"]["id"], k] = v
        start_idx += len(names_array)
    return (
        np.concatenate(names),
        np.array(feed_ids),
        np.concatenate(lats),
        np.concatenate(lons),
        remaps,
    )
