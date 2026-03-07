"""
Utilities for reducing collections of geographic points.

The main entrypoint, reduce_points, takes arrays of latitudes/longitudes and
returns a smaller set of representative points together with a mapping from
each original point to its representative. The guarantee is that every
original point is mapped to a representative at most ~max_distance_m away,
and the number of representatives is roughly minimal given that constraint.

This implementation follows a two-stage strategy:
- Use a coarse lat/lon grid to build a proximity graph connecting points
  that are within 2 * max_distance_m of each other (≈50 m for default).
- Then greedily consolidate high-degree points into clusters of radius
  max_distance_m (≈25 m), updating the graph/mapping as we go.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import networkx as nx
import numpy as np

from . import geo


def build_proximity_graph(
    lat: np.ndarray,
    lon: np.ndarray,
    link_radius_m: float,
) -> nx.Graph:
    """
    Build an undirected graph with an edge between every pair of points
    within link_radius_m of each other (great-circle distance).

    Uses a spatial grid so only nearby points are compared; expected time
    is linear in the number of points for typical distributions.

    Args:
        lat: 1D array of latitudes in degrees.
        lon: 1D array of longitudes in degrees.
        link_radius_m: Maximum distance in meters for an edge.

    Returns:
        G: networkx.Graph with nodes 0..n-1 and edges for pairs within link_radius_m.
    """
    lat = np.asarray(lat, dtype=np.float64).ravel()
    lon = np.asarray(lon, dtype=np.float64).ravel()
    n = lat.size
    G = nx.Graph()
    G.add_nodes_from(range(n))
    if n == 0:
        return G

    lat_rad = np.deg2rad(lat)
    lon_rad = np.deg2rad(lon)

    # 1 degree latitude ≈ 111.32 km everywhere; longitude degree length = 111.32 * cos(lat) km.
    M_PER_DEG_LAT = 111_320.0
    cell_deg = (link_radius_m * 1.5) / M_PER_DEG_LAT
    m_per_cell_lat = cell_deg * M_PER_DEG_LAT

    def cell(la_deg: float, lo_deg: float) -> Tuple[int, int]:
        gi = int(np.floor(la_deg / cell_deg))
        gj = int(np.floor(lo_deg / cell_deg))
        return (gi, gj)

    # How many cell steps in latitude direction to cover link_radius_m (same everywhere).
    n_di = max(1, int(np.ceil(link_radius_m / m_per_cell_lat)))

    # Build grid: (gi, gj) -> list of point indices.
    grid: Dict[Tuple[int, int], List[int]] = {}
    for i in range(n):
        key = cell(lat[i], lon[i])
        grid.setdefault(key, []).append(i)

    grid = {k: np.array(v, dtype=np.int64) for k, v in grid.items()}

    # Per cell: vectorized haversine between points in this cell and points in neighbor cells.
    for (ci, cj), I_arr in grid.items():
        if I_arr.size == 0:
            continue

        # Neighbor extent in j from this cell's center latitude.
        lat_center_rad = np.deg2rad((ci + 0.5) * cell_deg)
        cos_lat = max(np.cos(lat_center_rad), 1e-6)
        m_per_cell_lon = cell_deg * M_PER_DEG_LAT * cos_lat
        n_dj = max(1, int(np.ceil(link_radius_m / m_per_cell_lon)))

        neighbor_keys = [
            (ci + di, cj + dj)
            for di in range(-n_di, n_di + 1)
            for dj in range(-n_dj, n_dj + 1)
        ]

        for key in neighbor_keys:
            if key not in grid:
                continue
            J_arr = grid[key]

            lat1 = lat_rad[I_arr][:, None]
            lon1 = lon_rad[I_arr][:, None]
            lat2 = lat_rad[J_arr][None, :]
            lon2 = lon_rad[J_arr][None, :]
            # D:: len(I_arr) * len(J_arr)
            D = geo.haversine_m(lat1, lon1, lat2, lon2, degrees=False)

            mask = (D < link_radius_m) & (I_arr[:, None] < J_arr[None, :])
            ii, jj = np.where(mask)
            for a, b in zip(ii, jj):
                G.add_edge(int(I_arr[a]), int(J_arr[b]))

    return G


def reduce_points(
    lat: np.ndarray,
    lon: np.ndarray,
    max_distance_m: float = 25.0,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Reduce an array of points to a smaller set of representatives.

    Args:
        lat: 1D numpy array of latitudes in degrees.
        lon: 1D numpy array of longitudes in degrees.
        max_distance_m: Maximum allowed distance between an original point and
            its representative, in meters.

    Returns:
        new_lat: 1D array of representative latitudes.
        new_lon: 1D array of representative longitudes.
        mapping: 1D int array of length len(lat); mapping[i] is the index
            into (new_lat, new_lon) of the representative for point i.
    """
    n = lat.size

    lat_rad = np.deg2rad(lat)
    lon_rad = np.deg2rad(lon)

    # 1. Build proximity graph: edges between points within 2 * max_distance_m.
    G = build_proximity_graph(lat, lon, link_radius_m=2.0 * max_distance_m)

    rep_global = np.arange(n, dtype=np.int64)
    is_cluster_rep = np.zeros(n, dtype=bool)  # True if this node's position was overwritten as a centroid

    def centroid_for_cluster(cluster_idxs):
        return lat_rad[cluster_idxs].mean(), lon_rad[cluster_idxs].mean()

    def cluster_for_centroid(clat_rad, clon_rad, nearby_idx):
        """Points within max_distance_m of centroid that are still in the graph."""
        candidates = [nearby_idx] + [v for v in G.neighbors(nearby_idx)]
        candidates = [c for c in candidates if c in G]
        cluster = []
        for c in candidates:
            d = geo.haversine_m(
                clat_rad, clon_rad, lat_rad[c], lon_rad[c], degrees=False
            )
            if d <= max_distance_m:
                cluster.append(c)
        return cluster

    def greedy_cluster(center_idx):
        candidates = cluster_for_centroid(
            lat_rad[center_idx], lon_rad[center_idx], nearby_idx=center_idx
        )
        while True:
            centroid = centroid_for_cluster(candidates)
            new_candidates = cluster_for_centroid(*centroid, nearby_idx=center_idx)
            if set(new_candidates) == set(candidates):
                break
            candidates = new_candidates
        return candidates, centroid

    # 2. Greedy clustering: process by degree (high first). Merge only if all in cluster
    # are still in G and new centroid is >= min_rep_separation_m from all other current reps.
    deg = dict(G.degree())
    nodes_by_degree = sorted(G.nodes(), key=lambda u: deg[u], reverse=True)

    for u in nodes_by_degree:
        if u not in G:
            continue

        cluster, centroid = greedy_cluster(u)
        if len(cluster) <= 1:
            continue

        # Reject cluster if any member is already a cluster rep.
        if any(is_cluster_rep[c] for c in cluster):
            continue

        # Keep cluster[0] as representative; assign all in cluster to it; remove others from graph.
        new_idx = cluster[0]
        lat_rad[new_idx], lon_rad[new_idx] = centroid
        is_cluster_rep[new_idx] = True
        for c in cluster[1:]:
            rep_global[c] = new_idx
            # Remove node c and connect new_idx to nodes within 2*max_distance_m.
            neighbors = list(G.neighbors(c))
            candidates = set(neighbors) | {v for nb in neighbors for v in G.neighbors(nb)}
            for v in candidates:
                if (
                    v != new_idx
                    and v in G
                    and geo.haversine_m(
                        lat_rad[new_idx],
                        lon_rad[new_idx],
                        lat_rad[v],
                        lon_rad[v],
                        degrees=False,
                    )
                    < 2.0 * max_distance_m
                ):
                    G.add_edge(new_idx, v)
            G.remove_node(c)

    # 3. Compact representatives: map global indices to 0..M-1.
    unique_centers = sorted(set(rep_global))
    center_index = {g: i for i, g in enumerate(unique_centers)}
    mapping = np.array([center_index[int(g)] for g in rep_global], dtype=np.int64)

    new_lat = np.rad2deg(lat_rad[unique_centers])
    new_lon = np.rad2deg(lon_rad[unique_centers])
    return new_lat, new_lon, mapping
