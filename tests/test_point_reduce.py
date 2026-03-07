"""Tests for point_reduce.build_proximity_graph and reduce_points."""

import unittest

import numpy as np

from global_gtfs_graph import geo
from global_gtfs_graph.point_reduce import build_proximity_graph, reduce_points


class BuildProximityGraphTests(unittest.TestCase):
    """Edge cases for build_proximity_graph."""

    def test_empty_returns_empty_graph(self) -> None:
        lat = np.array([], dtype=np.float64)
        lon = np.array([], dtype=np.float64)
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 0)
        self.assertEqual(G.number_of_edges(), 0)

    def test_single_point_no_edges(self) -> None:
        lat = np.array([45.0])
        lon = np.array([-122.0])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 1)
        self.assertEqual(G.number_of_edges(), 0)

    def test_two_points_within_radius(self) -> None:
        # ~30 m apart in latitude at mid-lat (0.00027 deg ≈ 30 m).
        lat = np.array([45.0, 45.0 + 0.00027])
        lon = np.array([-122.0, -122.0])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 1)
        self.assertIn(0, G.adj[1])
        self.assertIn(1, G.adj[0])

    def test_two_points_beyond_radius(self) -> None:
        # ~100 m apart (0.0009 deg lat).
        lat = np.array([45.0, 45.0 + 0.0009])
        lon = np.array([-122.0, -122.0])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 0)

    def test_three_points_same_cell_all_linked(self) -> None:
        # Three points all within 50 m of each other (~20 m spacing).
        # 0.0002 deg ≈ 22 m.
        lat = np.array([45.0, 45.0 + 0.0002, 45.0 + 0.0001])
        lon = np.array([-122.0, -122.0, -122.0 - 0.0001])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 3)
        # Complete graph on 3 nodes = 3 edges.
        self.assertEqual(G.number_of_edges(), 3)

    def test_nodes_are_zero_indexed(self) -> None:
        lat = np.array([45.0, 45.0 + 0.0001])
        lon = np.array([-122.0, -122.0])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(set(G.nodes()), {0, 1})

    def test_ravels_1d_input(self) -> None:
        # Pass 2D (1, 2) and (1, 2) -> should be treated as 2 points.
        lat = np.array([[45.0, 45.0 + 0.0002]])
        lon = np.array([[-122.0, -122.0]])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 1)

    def test_high_latitude_more_longitude_cells(self) -> None:
        # At 60° N, 1 deg lon ≈ 55.5 km; so we need more cells in j to cover 50 m.
        # Two points ~30 m apart in longitude at 60°: dlon = 30/(111320*0.5) ≈ 0.00054 deg.
        lat = np.array([60.0, 60.0])
        lon = np.array([10.0, 10.0 + 0.00054])
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 1)

    def test_near_north_pole_longitude_cells(self) -> None:
        # at the poles, longitude degree length goes to zero, so we need many cells in j to cover 50 m.
        lat = np.array([89.999, 89.999])
        lon = np.array(
            [0.0, 1.0]
        )  # points even 1 degree apart in longitude are within 50 m at this latitude.
        G = build_proximity_graph(lat, lon, link_radius_m=50.0)
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 1)

    def test_fuzz_edges_match_naive_haversine(self) -> None:
        # Plop points in a small region; graph edges must match all-pairs haversine < radius.
        rng = np.random.default_rng(42)
        n = 800
        lat0, lon0 = 45.0, -122.0
        spread_deg = 0.002  # ~200 m; many pairs within 50 m
        lat = lat0 + rng.uniform(-spread_deg, spread_deg, size=n)
        lon = lon0 + rng.uniform(-spread_deg, spread_deg, size=n)
        link_radius_m = 50.0

        G = build_proximity_graph(lat, lon, link_radius_m=link_radius_m)

        expected_edges: set[tuple[int, int]] = set()
        for i in range(n):
            for j in range(i + 1, n):
                d = geo.haversine_m(lat[i], lon[i], lat[j], lon[j])
                if d < link_radius_m:
                    expected_edges.add((i, j))

        actual_edges = set((min(u, v), max(u, v)) for u, v in G.edges())
        self.assertEqual(actual_edges, expected_edges)
        self.assertGreaterEqual(
            G.number_of_edges(), n // 2
        )  # sanity check: should be many edges in this setup.


class ReducePointsTests(unittest.TestCase):
    """Edge cases for reduce_points."""

    def test_empty_returns_empty(self) -> None:
        lat = np.array([], dtype=np.float64)
        lon = np.array([], dtype=np.float64)
        new_lat, new_lon, mapping = reduce_points(lat, lon, max_distance_m=25.0)
        self.assertEqual(new_lat.tolist(), [])
        self.assertEqual(new_lon.tolist(), [])
        self.assertEqual(mapping.tolist(), [])

    def test_single_point_one_representative(self) -> None:
        lat = np.array([45.0])
        lon = np.array([-122.0])
        new_lat, new_lon, mapping = reduce_points(lat, lon, max_distance_m=25.0)
        # Output point and mapping should be identical to the input.
        self.assertTrue(np.allclose(new_lat, lat))
        self.assertTrue(np.allclose(new_lon, lon))
        self.assertEqual(mapping.tolist(), [0])

    def test_two_points_within_radius_reduced_to_one(self) -> None:
        # ~20 m apart; both within 25 m.
        lat = np.array([45.0, 45.0 + 0.00018])
        lon = np.array([-122.0, -122.0])
        new_lat, new_lon, mapping = reduce_points(lat, lon, max_distance_m=25.0)
        self.assertEqual(mapping.tolist(), [0, 0])
        self.assertTrue(np.allclose(new_lat, [45.0 + 0.00009], atol=1e-6))
        self.assertTrue(np.allclose(new_lon, [-122.0], atol=1e-6))

    def test_two_points_beyond_radius_two_representatives(self) -> None:
        # ~100 m apart; cannot merge with max_distance_m=25.
        lat = np.array([45.0, 45.0 + 0.0009])
        lon = np.array([-122.0, -122.0])
        new_lat, new_lon, mapping = reduce_points(lat, lon, max_distance_m=25.0)
        self.assertEqual(sorted(mapping.tolist()), [0, 1])
        self.assertTrue(np.allclose(new_lat, [45.0, 45.0 + 0.0009], atol=1e-6))
        self.assertTrue(np.allclose(new_lon, [-122.0, -122.0], atol=1e-6))

    def test_mapping_valid_and_each_point_near_representative(self) -> None:
        # Small deterministic cluster; mapping must be valid and each point within 2*max of rep.
        lat = np.array(
            [
                45.0,
                45.0 + 0.0001,
                45.0 - 0.0001,
                45.0 + 0.00015,
                45.0 - 0.00015,
            ]
        )
        lon = np.array(
            [
                -122.0,
                -122.0,
                -122.0,
                -122.0 + 0.00005,
                -122.0 - 0.00005,
            ]
        )
        max_distance_m = 25.0

        new_lat, new_lon, mapping = reduce_points(
            lat, lon, max_distance_m=max_distance_m
        )

        self.assertEqual(mapping.tolist(), [0, 0, 0, 0, 0])
        self.assertTrue(
            np.allclose(new_lat, [45.0], atol=1e-6)
        )  # centroid should be near 45.0
        self.assertTrue(
            np.allclose(new_lon, [-122.0], atol=1e-6)
        )  # centroid should be near -122.0

    def test_fuzz_reduces_points_and_respects_max_distance(self) -> None:
        # Many random points in a small region: should collapse to far fewer representatives,
        # and every point must be within max_distance_m of its representative.
        rng = np.random.default_rng(123)
        n = 200
        lat0, lon0 = 45.0, -122.0
        #  reasonable spread to get many points within 25 m, but not so tight that they all collapse to one.
        spread_deg = 0.001  # ~100 m; many points within 25 m, but not all within 25 m.
        lat = lat0 + rng.uniform(-spread_deg, spread_deg, size=n)
        lon = lon0 + rng.uniform(-spread_deg, spread_deg, size=n)
        max_distance_m = 25

        new_lat, new_lon, mapping = reduce_points(
            lat, lon, max_distance_m=max_distance_m
        )

        original_set = set((la, lo) for la, lo in zip(lat, lon))

        # Should be reduced substantially compared to the original count.
        self.assertLessEqual(len(new_lat), n // 4)

        # Every original point must be within max_distance_m of its representative.
        for i in range(n):
            rep_idx = int(mapping[i])
            d = geo.haversine_m(lat[i], lon[i], new_lat[rep_idx], new_lon[rep_idx])
            self.assertLessEqual(
                d,
                max_distance_m,
                msg=f"point {i} is {d:.3f} m from its representative",
            )

        # No two representatives within 10 m, except allow if both are from the original set (singletons).
        def rep_in_original(la: float, lo: float) -> bool:
            return (la, lo) in original_set

        num_reps = len(new_lat)
        for i in range(num_reps):
            for j in range(i + 1, num_reps):
                d = geo.haversine_m(new_lat[i], new_lon[i], new_lat[j], new_lon[j])
                if d >= max_distance_m:
                    continue
                # Allow if both reps are in the original point set (unchanged singletons).
                self.assertTrue(
                    rep_in_original(new_lat[i], new_lon[i])
                    and rep_in_original(new_lat[j], new_lon[j]),
                    msg=f"representatives {i} and {j} are {d:.3f} m apart and not both from original set",
                )
