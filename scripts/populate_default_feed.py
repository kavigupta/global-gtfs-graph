"""
Populate cached feed index, specs, and downloaded feed zips for the default feed version.
Run from repo root: python -m scripts.populate_default_feed
"""

from pathlib import Path

import tqdm

from global_gtfs_graph.calendar import standardize_calendars
from global_gtfs_graph.feed_graph import write_feed_graph
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

    services, start_common, end_common = standardize_calendars(
        feed_version=DEFAULT_FEED_VERSION, base=base
    )
    n_days = len(next(iter(services.values()), [])) if services else 0
    print(
        f"Standardized calendars: {len(services)} agencies, "
        f"common window {start_common}–{end_common} ({n_days} days)"
    )

    graph_count = 0
    for gtfs_info in tqdm.tqdm(
        all_gtfs_info(feed_version=DEFAULT_FEED_VERSION, base=base),
        desc="Writing graph files",
    ):
        r = gtfs_info["gtfs_result"]()
        if r["status"] != "success":
            continue
        write_feed_graph(r["content"], feed_id=gtfs_info["feed"]["id"], base=base)
        graph_count += 1
    print(f"Wrote {graph_count} feed graph files to data/graphs/")


if __name__ == "__main__":
    main()
