"""
Export all per-feed graphs into per-component shapefiles packaged as .zips.

- Points: one shapefile per component with all stops from feeds in that component
  - Output: data/geojson/components/component_<idx>_points.zip
  - Attributes: stop_id, name, feed_id, component (connected-component index)
- Lines: one shapefile per component with one MultiLineString per agency in that component
  - Output: data/geojson/components/component_<idx>_lines.zip
  - Attributes: agency_id

Connected components (the `component` field) are read from data/connected_components.json,
which is produced by scripts/combine_graphs.py.

Usage (from repo root):
    python scripts/graph_to_geojson.py                        # all feeds, per-component zips
    python scripts/graph_to_geojson.py data/                  # optional base data directory
    python scripts/graph_to_geojson.py data/graphs/foo.pb     # single graph -> foo_points.zip, foo_lines.zip

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


def _load_graph(path: Path) -> dict:
    """Load graph from .pb or .json; return dict with stops, lines, edges (derived from journeys when needed)."""
    if path.suffix.lower() == ".pb":
        pb = graph_pb2.FeedGraph()
        pb.ParseFromString(path.read_bytes())
        stops = [{"stop_id": s.stop_id, "name": s.name or "", "lat": s.lat, "lon": s.lon} for s in pb.stops]
        lines = [{"line_id": ln.line_id, "agency_id": getattr(ln, "agency_id", "") or ""} for ln in pb.lines]
        # Derive edges from full journeys: consecutive stop pairs in each journey.
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
        return {"stops": stops, "lines": lines, "edges": edges}
    data = json.loads(path.read_text())
    assert "stops" in data
    edges = data.get("edges")
    if edges is None and "journeys" in data:
        seen = set()
        edges = []
        for j in data["journeys"]:
            stops_seq = j.get("stops", [])
            lid = j.get("line_id", "")
            for i in range(len(stops_seq) - 1):
                sid, eid = stops_seq[i], stops_seq[i + 1]
                if sid != eid and (sid, eid, lid) not in seen:
                    seen.add((sid, eid, lid))
                    edges.append({"start_stop_id": sid, "end_stop_id": eid, "line_id": lid})
    return {"stops": data["stops"], "lines": data.get("lines", []), "edges": edges or []}


def _write_single_feed_shapefiles(data: dict, feed_id: str, out_dir: Path) -> None:
    """Write <feed_id>_points.zip and <feed_id>_lines.zip for one graph dict."""
    stops = data["stops"]
    lines = data.get("lines", [])
    edges = data.get("edges", [])
    line_id_to_agency = {ln["line_id"]: (ln.get("agency_id") or "") for ln in lines}
    stop_id_to_coords = {s["stop_id"]: (float(s["lon"]), float(s["lat"])) for s in stops}

    if stops:
        gdf_pts = gpd.GeoDataFrame(
            {"stop_id": [s["stop_id"] for s in stops], "name": [s.get("name") or "" for s in stops]},
            geometry=[Point(float(s["lon"]), float(s["lat"])) for s in stops],
            crs="EPSG:4326",
        )
        points_shp = out_dir / f"{feed_id}_points.shp"
        gdf_pts.to_file(points_shp, driver="ESRI Shapefile")
        _zip_shapefile(points_shp, out_dir / f"{feed_id}_points.zip")
        print(f"Wrote {out_dir / f'{feed_id}_points.zip'} ({len(stops)} points)")

    agency_segments = {}
    for e in edges:
        c1 = stop_id_to_coords.get(e["start_stop_id"])
        c2 = stop_id_to_coords.get(e["end_stop_id"])
        if c1 is None or c2 is None:
            continue
        aid = line_id_to_agency.get(e.get("line_id"), "") or ""
        agency_segments.setdefault(aid, []).append((c1, c2))
    if agency_segments:
        multilinestrings = [MultiLineString([LineString([c1, c2]) for c1, c2 in segs]) for segs in [agency_segments[a] for a in sorted(agency_segments)]]
        gdf_lines = gpd.GeoDataFrame(
            {"agency_id": sorted(agency_segments)},
            geometry=multilinestrings,
            crs="EPSG:4326",
        )
        lines_shp = out_dir / f"{feed_id}_lines.shp"
        gdf_lines.to_file(lines_shp, driver="ESRI Shapefile")
        _zip_shapefile(lines_shp, out_dir / f"{feed_id}_lines.zip")
        print(f"Wrote {out_dir / f'{feed_id}_lines.zip'} ({len(agency_segments)} agencies)")


def main():
    if len(sys.argv) > 2:
        print("Usage: python scripts/graph_to_geojson.py [data-dir | path-to-graph.pb]", file=sys.stderr)
        sys.exit(1)

    arg = Path(sys.argv[1]) if len(sys.argv) == 2 else None
    if arg is not None and arg.is_file():
        # Single graph file
        path = arg
        if path.suffix.lower() not in (".pb", ".json"):
            print("Single-file input must be a .pb or .json graph.", file=sys.stderr)
            sys.exit(1)
        base = path.parent.parent if path.parent.name == "graphs" else path.parent
        out_dir = base / "geojson"
        out_dir.mkdir(parents=True, exist_ok=True)
        data = _load_graph(path)
        _write_single_feed_shapefiles(data, path.stem, out_dir)
        return

    base = _default_data_dir() if arg is None else arg
    if not base.is_dir() or not base.exists():
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

        # Lines: derive segments from journeys (consecutive stops), group by agency_id
        segs_for_comp = segments_by_comp.setdefault(component, {})
        line_to_agency = {ln.line_id: (ln.agency_id or "") for ln in pb.lines}
        for j in pb.journeys:
            agency_id = line_to_agency.get(j.line_id, "") or ""
            for i in range(len(j.stops) - 1):
                c1 = stop_coords.get(j.stops[i])
                c2 = stop_coords.get(j.stops[i + 1])
                if c1 is None or c2 is None:
                    continue
                segs_for_comp.setdefault(agency_id, []).append((c1, c2))

    out_dir = base / "geojson" / "components"
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
