"""
Generate a static image from a .pb graph file: stops as black points, edges colored by hash of line name.
Requires: pip install matplotlib (or pip install global-gtfs-graph[plot])
"""

import argparse
import hashlib
import sys
from pathlib import Path

from global_gtfs_graph import graph_pb2

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib is required. Install with: pip install matplotlib")


def load_pb_graph(path: Path) -> graph_pb2.FeedGraph:
    pb = graph_pb2.FeedGraph()
    pb.ParseFromString(path.read_bytes())
    return pb


def line_name_to_color(line_name: str, cmap_name: str = "tab20"):
    """Deterministic color from line name via hash, using a matplotlib colormap."""
    h = hashlib.sha256(line_name.encode("utf-8")).hexdigest()
    # Use first 8 hex chars as index into colormap (wraps if needed)
    idx = int(h[:8], 16) % 256
    cmap = plt.get_cmap(cmap_name)
    return cmap(idx / 255.0)


def render_graph(pb: graph_pb2.FeedGraph, out_path: Path, dpi: int = 150) -> None:
    stop_coords = {s.stop_id: (s.lon, s.lat) for s in pb.stops}
    line_id_to_name = {ln.line_id: ln.name or ln.line_id for ln in pb.lines}

    fig, ax = plt.subplots(figsize=(12, 10))
    ax.set_aspect("equal")

    # Draw edges first (so points sit on top), colored by hash of line name
    for e in pb.edges:
        start = stop_coords.get(e.start_stop_id)
        end = stop_coords.get(e.end_stop_id)
        if start is None or end is None:
            continue
        name = line_id_to_name.get(e.line_id, str(e.line_id))
        color = line_name_to_color(name)
        ax.plot([start[0], end[0]], [start[1], end[1]], color=color, linewidth=0.4, alpha=0.8)

    # Draw stops as black points
    lons = [s.lon for s in pb.stops]
    lats = [s.lat for s in pb.stops]
    ax.scatter(lons, lats, s=0.5, c="black", alpha=0.9)

    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a static image from a .pb graph file.",
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to .pb graph file",
    )
    parser.add_argument(
        "output",
        type=Path,
        nargs="?",
        default=None,
        help="Output PNG path (default: data/images/<stem>.png)",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=150,
        help="DPI for output image (default: 150)",
    )
    args = parser.parse_args()

    path = args.input
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)
    if path.suffix.lower() != ".pb":
        print("Input must be a .pb graph file.", file=sys.stderr)
        sys.exit(1)

    out_path = args.output if args.output is not None else Path("data/images") / f"{path.stem}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pb = load_pb_graph(path)
    render_graph(pb, out_path, dpi=args.dpi)
    print(f"Wrote {out_path} ({len(pb.stops)} stops, {len(pb.edges)} edges)")


if __name__ == "__main__":
    main()
