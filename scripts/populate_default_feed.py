"""
Populate cached feed index and specs for the default feed version.
Run from repo root: python -m scripts.populate_default_feed
"""

from pathlib import Path

import tqdm

from global_gtfs_graph.feeds import FeedVersion, gtfs_list, read_gtfs_spec

DEFAULT_FEED_VERSION = FeedVersion(
    name="2026-02-22",
    git_hash="98c4d9faf5b214e97fcb1e5051ce8569cb4ba7b8",
)


def main():
    base = Path(__file__).resolve().parent.parent / "data"
    paths = gtfs_list(feed_version=DEFAULT_FEED_VERSION, base=base)
    for path in tqdm.tqdm(paths, desc="Fetching specs"):
        read_gtfs_spec(path, feed_version=DEFAULT_FEED_VERSION, base=base)
    print(f"Populated {len(paths)} feed specs for {DEFAULT_FEED_VERSION.name}")


if __name__ == "__main__":
    main()
