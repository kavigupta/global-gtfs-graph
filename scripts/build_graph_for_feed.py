"""
Build a single .pb graph file for one feed.

This is a thin wrapper around global_gtfs_graph.feed_graph.write_feed_graph,
so it uses the same calendar/timezone normalization as the bulk pipeline.

Usage (from repo root):
    python scripts/build_graph_for_feed.py FEED_ID
    python scripts/build_graph_for_feed.py FEED_ID --data-dir data

Example:
    python scripts/build_graph_for_feed.py f-u-nl

The .pb file is written to <data-dir>/graphs/<FEED_ID>.pb (with a sanitized
filename when FEED_ID contains characters that are not filename-safe). This
script only reads from the existing cached GTFS zip under data/feeds/ and
does NOT hit the Transitland API.
"""

import argparse
import zipfile
from pathlib import Path

from global_gtfs_graph import gtfs_io
from global_gtfs_graph.feeds import _feed_zip_path
from global_gtfs_graph.feed_graph import write_feed_graph


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a single FeedGraph .pb file for one Transitland feed_id "
            "(same format as scripts/populate_default_feed.py)."
        ),
    )
    parser.add_argument(
        "feed_id",
        help="Transitland feed_id (e.g. f-u-nl)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default="data/",
        help=(
            "Base data directory (default: package DEFAULT_DATA_DIR, usually "
            "the repo's data/ directory)."
        ),
    )
    args = parser.parse_args()

    base: Path = args.data_dir
    base.mkdir(parents=True, exist_ok=True)

    zip_path = _feed_zip_path(args.feed_id, base=base)
    if not zip_path.exists():
        raise SystemExit(
            f"GTFS zip for feed_id {args.feed_id!r} not found at {zip_path}. "
            "Run scripts/populate_default_feed.py first (or download the zip manually)."
        )

    with zipfile.ZipFile(zip_path) as zf:
        gtfs = {
            name: gtfs_io.read_try_multiple_encodings(lambda n=name: zf.open(n))
            for name in zf.namelist()
        }

    out_path = write_feed_graph(gtfs, feed_id=args.feed_id, base=base)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

