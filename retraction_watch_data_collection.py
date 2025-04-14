"""
This file contains methods to collect information about retracted items from Retraction Watch. This
is available as a bulk download from Retraction Watch via GitLab.

Functions overview:
get_gitlab_file_and_sha: download file from GitLab and validate using provided SHA value
"""
from datetime import date
import requests
import os
import hashlib
import base64


def get_gitlab_file_and_sha(path, expected_commit_id):
    if not os.path.exists(path):
        base_dir = os.path.dirname(path)
        print(f"Creating directory for {path}")
        os.makedirs(base_dir, exist_ok=True)

    response = requests.get(
        "https://gitlab.com/api/v4/projects/crossref%2Fretraction-watch-data/repository/files/retraction_watch.csv?ref=main"
    )
    response.raise_for_status()

    # Get the response as JSON
    content = response.json()

    # Get the filename, SHA256, and commit ID
    file_name = content['file_name']
    sha_256 = content['content_sha256']
    commit_id = content['commit_id']

    print(f"GitLab File Name: {file_name}")
    print(f"Commit ID: {commit_id}")
    print(f"SHA256: {sha_256}")

    assert commit_id == expected_commit_id, \
        "Commit ID and expected commit ID do not match. Are you using the most recent commit?"

    # Decode the content and write to disk
    data = base64.b64decode(content["content"])
    with open(f"{path}/{str(date.today())}_{file_name}", "wb") as f:
        f.write(data)
        f.close()

    # Write GitLab SHA256 to disk
    with open(f"{path}/sha256_{file_name}", "wt") as f:
        f.write(sha_256)
        f.close()

    # Use hashlib to compute the SHA256 of the stored file
    with open(f"{path}/{str(date.today())}_{file_name}", "rb") as f:
        file_sha256 = hashlib.file_digest(f, 'sha256').hexdigest()
        f.close()
    print(f"Computed SHA256: {file_sha256}")

    assert file_sha256 == sha_256, "Expected SHA for file and computed SHA for file do not match."


def main():
    data_path = f"data"
    commit_id = "66e357c8ec0d2692686bc82864be65afaf16e1d8"
    get_gitlab_file_and_sha(data_path, commit_id)


if __name__ == "__main__":
    main()
