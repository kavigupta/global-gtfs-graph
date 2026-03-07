"""
Export all stops from data/graphs/*.pb to a shapefile with agency info.

Output: data/exports/all_points.zip (shapefile)
  Attributes: feed_id, stop_id, name, lat, lon, agency (first agency serving the stop)

Each stop includes the first agency that serves it (via journeys).

Usage (from repo root):
    python scripts/export_points.py
    python scripts/export_points.py --output data/exports/all_points.zip

Requires: pip install geopandas (or pip install global-gtfs-graph[shapefile])
"""

import argparse
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

from global_gtfs_graph import graph_pb2

try:
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError:
    sys.exit("geopandas is required. Install with: pip install geopandas")


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def _zip_shapefile(shp_path: Path, zip_path: Path) -> None:
    """Zip all components of a shapefile and remove the originals."""
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export all stops from graph .pb files to a shapefile with agency info.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Base data directory (default: repo data/)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output shapefile zip path (default: data/exports/all_points.zip)",
    )
    args = parser.parse_args()

    base = args.data_dir if args.data_dir else _default_data_dir()
    graphs_dir = base / "graphs"
    if not graphs_dir.exists():
        raise SystemExit(f"Graphs directory not found: {graphs_dir}")

    out_path = args.output if args.output else base / "exports" / "all_points.zip"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    geoms = []

    pb_files = sorted(graphs_dir.glob("*.pb"))
    for pb_path in pb_files:
        feed_id = pb_path.stem
        pb = graph_pb2.FeedGraph()
        pb.ParseFromString(pb_path.read_bytes())

        line_to_agency = {ln.line_id: ln.agency_id or "" for ln in pb.lines}

        stop_agencies: dict[int, set[str]] = defaultdict(set)
        for j in pb.journeys:
            agency = line_to_agency.get(j.line_id, "")
            for sid in j.stops:
                stop_agencies[sid].add(agency)

        for s in pb.stops:
            agencies = sorted(stop_agencies.get(s.stop_id, set()))
            rows.append({
                "feed_id": feed_id,
                "stop_id": s.stop_id,
                "name": (s.name or "")[:80],  # Shapefile field limit
                "agency": agencies[0] if agencies else "",
            })
            geoms.append(Point(s.lon, s.lat))

    if not rows:
        print("No points found.")
        return

    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    shp_path = out_path.with_suffix(".shp")
    gdf.to_file(shp_path, driver="ESRI Shapefile")
    _zip_shapefile(shp_path, out_path)
    print(f"Wrote {out_path} ({len(rows)} points from {len(pb_files)} feeds)")


if __name__ == "__main__":
    main()
