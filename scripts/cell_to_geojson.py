"""
Convert a single cell .pb (e.g. data/combined_graphs/cells/50_3.pb) to two GeoJSONs:
  - <stem>_points.geojson  (stops as Point features)
  - <stem>_lines.geojson   (journey segments as LineString features, one per line/agency)

Usage (from repo root):
  python scripts/cell_to_geojson.py data/combined_graphs/cells/50_3.pb
  python scripts/cell_to_geojson.py data/combined_graphs/cells/50_3.pb --out-dir /tmp

By default, output files are written under data/cell_geojson/, or to --out-dir if given.
Requires: pip install geopandas
"""

import argparse
from pathlib import Path

from global_gtfs_graph import graph_pb2

try:
    import geopandas as gpd
    from shapely.geometry import LineString, Point
except ImportError:
    raise SystemExit("geopandas is required. Install with: pip install geopandas")


def load_cell_pb(path: Path) -> tuple[list, list, list]:
    """Load cell .pb; return (stops, lines, edges). Edges from journeys."""
    pb = graph_pb2.FeedGraph()
    pb.ParseFromString(path.read_bytes())
    stops = [
        {
            "stop_id": s.stop_id,
            "name": s.name or "",
            "lat": s.lat,
            "lon": s.lon,
            # minutes_used is optional in the proto; default to 0 if unset.
            "minutes_used": getattr(s, "minutes_used", 0),
        }
        for s in pb.stops
    ]
    lines = [
        {"line_id": ln.line_id, "agency_id": getattr(ln, "agency_id", "") or ""}
        for ln in pb.lines
    ]
    seen = set()
    edges = []
    for j in pb.journeys:
        lid = j.line_id
        for i in range(len(j.stops) - 1):
            sid, eid = j.stops[i], j.stops[i + 1]
            if sid == eid:
                continue
            key = (sid, eid, lid)
            if key in seen:
                continue
            seen.add(key)
            edges.append({"start_stop_id": sid, "end_stop_id": eid, "line_id": lid})
    return stops, lines, edges


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert cell .pb to points + lines GeoJSON")
    parser.add_argument("pb_path", type=Path, help="Path to cell .pb (e.g. data/combined_graphs/cells/50_3.pb)")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default: data/cell_geojson next to combined_graphs)",
    )
    args = parser.parse_args()

    path = args.pb_path.resolve()
    if not path.is_file() or path.suffix.lower() != ".pb":
        raise SystemExit(f"Not a .pb file: {path}")

    if args.out_dir is not None:
        out_dir = args.out_dir.resolve()
    else:
        # For standard layout data/combined_graphs/cells/<cell>.pb,
        # write to data/cell_geojson.
        parent = path.parent
        if parent.name == "cells" and parent.parent.name == "combined_graphs":
            data_dir = parent.parent.parent
            out_dir = (data_dir / "cell_geojson").resolve()
        else:
            # Fallback: sibling of the .pb
            out_dir = (path.parent / "cell_geojson").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = path.stem

    stops, lines, edges = load_cell_pb(path)
    line_id_to_agency = {ln["line_id"]: (ln.get("agency_id") or "") for ln in lines}
    stop_id_to_coords = {s["stop_id"]: (float(s["lon"]), float(s["lat"])) for s in stops}

    if stops:
        gdf_pts = gpd.GeoDataFrame(
            {
                "stop_id": [s["stop_id"] for s in stops],
                "name": [s.get("name") or "" for s in stops],
                # minutes_used as stored in the cell protobuf (0 inside the cell,
                # >0 outside, always < MAX_TRAVEL_MINUTES).
                "minutes_used": [int(s.get("minutes_used", 0)) for s in stops],
            },
            geometry=[Point(float(s["lon"]), float(s["lat"])) for s in stops],
            crs="EPSG:4326",
        )
        pts_path = out_dir / f"{stem}_points.geojson"
        gdf_pts.to_file(pts_path, driver="GeoJSON")
        print(f"Wrote {pts_path} ({len(stops)} points)")

    agency_segments: dict[str, list[tuple[tuple[float, float], tuple[float, float]]]] = {}
    for e in edges:
        c1 = stop_id_to_coords.get(e["start_stop_id"])
        c2 = stop_id_to_coords.get(e["end_stop_id"])
        if c1 is None or c2 is None:
            continue
        aid = line_id_to_agency.get(e.get("line_id"), "") or ""
        agency_segments.setdefault(aid, []).append((c1, c2))

    if agency_segments:
        rows = []
        for aid in sorted(agency_segments):
            for (c1, c2) in agency_segments[aid]:
                rows.append({"agency_id": aid, "geometry": LineString([c1, c2])})
        gdf_lines = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        lines_path = out_dir / f"{stem}_lines.geojson"
        gdf_lines.to_file(lines_path, driver="GeoJSON")
        print(f"Wrote {lines_path} ({len(gdf_lines)} segments)")
    else:
        print("No edges; skipping lines GeoJSON.")


if __name__ == "__main__":
    main()
