"""
HOW TO GET GITLAB SHA VALUE?

This file contains methods to collect information about retracted items from Retraction Watch. This
is available as a bulk download from Retraction Watch.

Functions overview:
download_and_validate: download file from GitLab and validate using provided SHA value
"""
from datetime import date
import requests
import os
import hashlib
import gitlab


def get_gitlab_file_sha():
    gl = gitlab.Gitlab()


def download_and_validate(url, path, expected_hash):

    response = requests.get(url)

    response.raise_for_status()

    if not os.path.exists(path):
        base_dir = os.path.dirname(path)
        print(f"Creating directory for {path}")
        os.makedirs(base_dir, exist_ok=True)

    with open(path, mode="wb") as f:
        f.write(response.content)

    with open(path, mode="rb") as f:
        data = f.read()
        computed_hash = hashlib.sha256(data).hexdigest()

    if computed_hash != expected_hash:
        print(f"WARNING: Computed hash does not match expected has for {path}")
    else:
        print(f"INFO: Computed hash matches expected hash for {path}")


def main():
    retraction_watch_url = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv"
    retraction_watch_expected_hash = "44b418647415cea82425fa7a2d29d6c397ff272c"
    retraction_watch_path = f"data/retraction_watch_{str(date.today())}.csv"
    download_and_validate(retraction_watch_url, retraction_watch_path, retraction_watch_expected_hash)


if __name__ == "__main__":
    main()
