"""
Populate cached feed index, specs, and downloaded feed zips for the default feed version.
Run from repo root: python -m scripts.populate_default_feed
"""

from pathlib import Path

import tqdm

from global_gtfs_graph.feeds import (
    FeedVersion,
    all_gtfs_info,
    gtfs_list,
    read_gtfs_spec,
)

DEFAULT_FEED_VERSION = FeedVersion(
    name="2026-02-22",
    git_hash="98c4d9faf5b214e97fcb1e5051ce8569cb4ba7b8",
)


def main():
    base = Path(__file__).resolve().parent.parent / "data"
    paths = gtfs_list(feed_version=DEFAULT_FEED_VERSION, base=base)
    for path in tqdm.tqdm(paths, desc="Fetching specs"):
        read_gtfs_spec(path, feed_version=DEFAULT_FEED_VERSION, base=base)

    count = 0
    for gtfs_info in all_gtfs_info(feed_version=DEFAULT_FEED_VERSION, base=base):
        gtfs_info["gtfs_result"]()
        count += 1
    print(
        f"Populated {len(paths)} feed specs and {count} feed zips for {DEFAULT_FEED_VERSION.name}"
    )


if __name__ == "__main__":
    main()
