"""GTFS feed listing and download from Transitland/GitHub."""

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests

from . import gtfs_io

DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


@dataclass(frozen=True)
class FeedVersion:
    """Identifies a version of the feed index (e.g. Transitland Atlas tree)."""

    name: str
    git_hash: str
    repo: str = "transitland/transitland-atlas"

    def data_dir(self, base: Path = DEFAULT_DATA_DIR) -> Path:
        """Directory under data/ where this version's index and specs are stored."""
        return base / self.name


def _feed_paths_file(feed_version: FeedVersion, base: Path = DEFAULT_DATA_DIR) -> Path:
    return feed_version.data_dir(base) / "feed_paths.json"


def _version_file(feed_version: FeedVersion, base: Path = DEFAULT_DATA_DIR) -> Path:
    return feed_version.data_dir(base) / "version.txt"


def gtfs_list(
    feed_version: FeedVersion,
    base: Path = DEFAULT_DATA_DIR,
) -> list[str]:
    """
    List feed paths (e.g. feeds/...) for the given FeedVersion.
    Downloads and caches the list under data/<name>/; reuses cache when version matches.
    """
    paths_file = _feed_paths_file(feed_version, base)
    version_file = _version_file(feed_version, base)

    if paths_file.exists():
        if version_file.exists() and version_file.read_text().strip() == feed_version.git_hash:
            return json.loads(paths_file.read_text())

    url_api = (
        f"https://api.github.com/repos/{feed_version.repo}/git/trees/"
        f"{feed_version.git_hash}?recursive=1"
    )
    tree = requests.get(url_api).json()["tree"]
    feeds = [x["path"] for x in tree if x["path"].startswith("feeds/")]

    data_dir = feed_version.data_dir(base)
    data_dir.mkdir(parents=True, exist_ok=True)
    version_file.write_text(feed_version.git_hash + "\n")
    paths_file.write_text(json.dumps(feeds, indent=2))

    return feeds


def read_gtfs_spec(
    feed_path: str,
    feed_version: FeedVersion,
    base: Path = DEFAULT_DATA_DIR,
) -> dict:
    """
    Load the GTFS spec JSON for a feed path. Cached under data/<name>/specs/.
    """
    specs_dir = feed_version.data_dir(base) / "specs"
    safe_name = feed_path.replace("/", "_")
    spec_file = specs_dir / safe_name

    if spec_file.exists():
        return json.loads(spec_file.read_text())

    url = (
        f"https://raw.githubusercontent.com/{feed_version.repo}/"
        f"{feed_version.git_hash}/{feed_path}"
    )
    spec = requests.get(url).json()

    specs_dir.mkdir(parents=True, exist_ok=True)
    spec_file.write_text(json.dumps(spec, indent=2))

    return spec


def api_key():
    with open(os.path.expanduser("~/.transitland")) as f:
        api_key = f.read().strip()
    return api_key


def _feed_zip_path(feed_id: str, base: Path = DEFAULT_DATA_DIR) -> Path:
    """Cache path for a feed's raw zip. feed_id is sanitized for use as a filename."""
    safe = re.sub(r"[^\w\-.]", "_", feed_id)
    return base / "feeds" / f"{safe}.zip"


def read_gtfs_from_feed_id_raw(
    feed_id: str,
    base: Path = DEFAULT_DATA_DIR,
) -> dict:
    """
    Download raw GTFS zip for a Transitland feed_id. Cached under data/feeds/<feed_id>.zip.
    """
    zip_path = _feed_zip_path(feed_id, base)

    if zip_path.exists():
        return {"status": "success", "content": zip_path.read_bytes()}

    try:
        result = requests.get(
            f"https://transit.land/api/v2/rest/feeds/{feed_id}/download_latest_feed_version",
            params=dict(api_key=api_key()),
        )
        if result.status_code != 200:
            return {
                "status": "failure",
                "reason": f"status code {result.status_code}; content: {result.content}",
            }
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(result.content)
        return {"status": "success", "content": result.content}
    except requests.exceptions.Timeout:
        return {"status": "failure", "reason": "timeout"}
    except requests.exceptions.ConnectionError:
        return {"status": "failure", "reason": "connection error"}
    except requests.exceptions.ContentDecodingError:
        return {"status": "failure", "reason": "content decoding error"}
    except requests.exceptions.ChunkedEncodingError:
        return {"status": "failure", "reason": "chunked encoding error"}


def read_gtfs_from_feed_id(
    feed_id: str,
    base: Path = DEFAULT_DATA_DIR,
) -> dict:
    """
    Load GTFS feed by Transitland feed_id; returns parsed CSV contents as DataFrames.
    Raw zip is cached under data/feeds/<feed_id>.zip.
    """
    res = read_gtfs_from_feed_id_raw(feed_id, base=base)
    if res["status"] == "failure":
        return res
    zip_buf = io.BytesIO(res["content"])
    try:
        zip_file = zipfile.ZipFile(zip_buf)
    except zipfile.BadZipFile:
        return {"status": "failure", "reason": "bad zip file"}
    return {
        "status": "success",
        "content": {
            name: gtfs_io.read_try_multiple_encodings(lambda n=name: zip_file.open(n))
            for name in zip_file.namelist()
        },
    }


def all_gtfs_info(
    feed_version: FeedVersion,
    base: Path = DEFAULT_DATA_DIR,
):
    import tqdm.auto as tqdm

    urls = gtfs_list(feed_version=feed_version, base=base)
    for url in tqdm.tqdm(urls):
        spec = read_gtfs_spec(url, feed_version=feed_version, base=base)
        for feed in spec["feeds"]:
            yield dict(
                feed=feed,
                gtfs_result=lambda feed=feed, base=base: read_gtfs_from_feed_id(
                    feed["id"], base=base
                ),
            )


def all_failures(
    feed_version: FeedVersion,
    base: Path = DEFAULT_DATA_DIR,
):
    bad_feeds = []
    for res in all_gtfs_info(feed_version=feed_version, base=base):
        gtfs = res["gtfs_result"]()
        if gtfs["status"] == "failure":
            bad_feeds.append((res["feed"], gtfs["reason"]))
    return bad_feeds
