"""
Export all per-feed graphs into per-component shapefiles packaged as .zips.

- Points: one shapefile per component with all stops from feeds in that component
  - Filenames: component_<idx>_points.zip
  - Attributes: stop_id, name, feed_id, component (connected-component index)
- Lines: one shapefile per component with one MultiLineString per agency in that component
  - Filenames: component_<idx>_lines.zip
  - Attributes: agency_id

Connected components (the `component` field) are read from data/connected_components.json,
which is produced by scripts/combine_graphs.py.

Usage (from repo root):
    python scripts/graph_to_geojson.py            # uses data/ as base
    python scripts/graph_to_geojson.py data/alt   # optional base data directory

Requires: pip install geopandas (or pip install global-gtfs-graph[shapefile])
"""

import json
import sys
import zipfile
from pathlib import Path

from global_gtfs_graph import graph_pb2

try:
    import geopandas as gpd
    from shapely.geometry import Point, LineString, MultiLineString
except ImportError:
    sys.exit("geopandas is required. Install with: pip install geopandas")


def _zip_shapefile(shp_path: Path, zip_path: Path) -> None:
    """Zip all components of a shapefile (e.g. stem.shp, stem.shx, stem.dbf, stem.prj)."""
    stem = shp_path.stem
    parent = shp_path.parent
    exts = [".shp", ".shx", ".dbf", ".prj", ".cpg"]
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for ext in exts:
            f = parent / f"{stem}{ext}"
            if f.exists():
                zf.write(f, f.name)
    for ext in exts:
        f = parent / f"{stem}{ext}"
        if f.exists():
            f.unlink()


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def main():
    if len(sys.argv) > 2:
        print("Usage: python scripts/graph_to_geojson.py [data-dir]", file=sys.stderr)
        sys.exit(1)

    base = _default_data_dir() if len(sys.argv) == 1 else Path(sys.argv[1])
    if not base.exists():
        print(f"Data directory not found: {base}", file=sys.stderr)
        sys.exit(1)

    components_path = base / "connected_components.json"
    if not components_path.exists():
        print(f"Connected components file not found: {components_path}", file=sys.stderr)
        sys.exit(1)

    comp_data = json.loads(components_path.read_text())
    components = comp_data.get("components", [])
    feed_to_component: dict[str, int] = {}
    for idx, feeds in enumerate(components):
        for feed_id in feeds:
            feed_to_component[feed_id] = idx

    graphs_dir = base / "graphs"
    if not graphs_dir.exists():
        print(f"Graphs directory not found: {graphs_dir}", file=sys.stderr)
        sys.exit(1)

    # Accumulate all points and segments from all feed graphs, grouped by component
    points_by_comp: dict[int, dict[str, list]] = {}
    segments_by_comp: dict[int, dict[str, list[tuple[tuple[float, float], tuple[float, float]]]]] = {}

    pb_files = sorted(graphs_dir.glob("*.pb"))
    for pb_path in pb_files:
        feed_id = pb_path.stem
        component = feed_to_component.get(feed_id, -1)
        if component < 0:
            # Feed not present in connected_components; skip for now.
            continue

        pb = graph_pb2.FeedGraph()
        pb.ParseFromString(pb_path.read_bytes())

        stop_coords = {s.stop_id: (float(s.lon), float(s.lat)) for s in pb.stops}

        # Points
        pts = points_by_comp.setdefault(
            component,
            {"geom": [], "stop_id": [], "name": [], "feed_id": [], "component": []},
        )
        for s in pb.stops:
            pts["geom"].append(Point(float(s.lon), float(s.lat)))
            pts["stop_id"].append(s.stop_id)
            pts["name"].append(s.name or "")
            pts["feed_id"].append(feed_id)
            pts["component"].append(component)

        # Lines: group segments by agency_id within this component
        segs_for_comp = segments_by_comp.setdefault(component, {})
        line_to_agency = {ln.line_id: (ln.agency_id or "") for ln in pb.lines}
        for e in pb.edges:
            c1 = stop_coords.get(e.start_stop_id)
            c2 = stop_coords.get(e.end_stop_id)
            if c1 is None or c2 is None:
                continue
            agency_id = line_to_agency.get(e.line_id, "") or ""
            segs_for_comp.setdefault(agency_id, []).append((c1, c2))

    out_dir = base / "geojson"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not points_by_comp:
        print("No points found to export.")
    if not segments_by_comp:
        print("No edges with valid stops; skipping lines shapefiles.")

    # Points and lines shapefiles per component
    for component, pts in sorted(points_by_comp.items()):
        comp_segments = segments_by_comp.get(component, {})

        # Points shapefile for this component
        if pts["geom"]:
            gdf_pts = gpd.GeoDataFrame(
                {
                    "stop_id": pts["stop_id"],
                    "name": pts["name"],
                    "feed_id": pts["feed_id"],
                    "component": pts["component"],
                },
                geometry=pts["geom"],
                crs="EPSG:4326",
            )
            points_shp = out_dir / f"component_{component}_points.shp"
            gdf_pts.to_file(points_shp, driver="ESRI Shapefile")
            points_zip = out_dir / f"component_{component}_points.zip"
            _zip_shapefile(points_shp, points_zip)
            print(f"Wrote {points_zip} (component {component}, {len(pts['geom'])} points)")

        # Lines shapefile for this component
        if comp_segments:
            multilinestrings = []
            agency_ids = []
            for agency_id in sorted(comp_segments.keys()):
                segments = comp_segments[agency_id]
                lines_geom = [LineString([c1, c2]) for c1, c2 in segments]
                multilinestrings.append(MultiLineString(lines_geom))
                agency_ids.append(agency_id)

            gdf_lines = gpd.GeoDataFrame(
                {"agency_id": agency_ids},
                geometry=multilinestrings,
                crs="EPSG:4326",
            )
            lines_shp = out_dir / f"component_{component}_lines.shp"
            gdf_lines.to_file(lines_shp, driver="ESRI Shapefile")
            lines_zip = out_dir / f"component_{component}_lines.zip"
            _zip_shapefile(lines_shp, lines_zip)
            total_segments = sum(len(v) for v in comp_segments.values())
            print(
                f"Wrote {lines_zip} (component {component}, {len(agency_ids)} agencies, total segments {total_segments})"
            )


if __name__ == "__main__":
    main()
