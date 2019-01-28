"""Create a sentinel sqlite database index."""
import os

import reproduce

SENTINEL_CSV_INDEX_GS = 'gs://gcp-public-data-sentinel-2/index.csv.gz'
IAM_TOKEN_PATH = 'ecoshard-202992-key.json'

WORKSPACE_DIR = 'workspace'


def main():
    """Entry point."""
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass

    reproduce.utils.google_bucket_fetcher(
        SENTINEL_CSV_INDEX_GS, IAM_TOKEN_PATH)


if __name__ == '__main__':
    main()
