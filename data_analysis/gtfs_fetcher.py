import os

import logging

# required for pandas to read csv from aws
import boto3
from botocore import UNSIGNED
from botocore.client import Config
#from s3path import S3Path
#boto3.setup_default_session(signature_version=UNSIGNED)
import pandas as pd
import pendulum
from tqdm import tqdm

from utils import s3_csv_reader


BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
# https://stackoverflow.com/questions/34865927/can-i-use-boto3-anonymously
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
#BASE_PATH = S3Path(f"/{BUCKET_PUBLIC}")

#GTFS_PATH = BASE_PATH / "cta_schedule_zipfiles_raw"



# filename = f'cta_schedule_zipfiles_raw/google_transit_{today}.zip'

class GTFSFetcher:
    def __init__(self):
        files = s3.list_objects_v2(Bucket=BUCKET_PUBLIC, Prefix='cta_schedule_zipfiles_raw/')
        self.unique_files = {}
        for fc in files['Contents']:
            key = fc['ETag']
            filename = fc['Key'].split('/')[1]
            size = fc['Size']
            self.unique_files.setdefault(key, []).append((filename, size))
        for v in self.unique_files.values():
            v.sort()

    def list(self):
        tups = []
        for v in self.unique_files.values():
            tups.append(v[0])
        tups.sort()
        return tups


if __name__ == "__main__":
    #files = s3.list_objects_v2(Bucket=BUCKET_PUBLIC, Prefix='cta_schedule_zipfiles_raw/')
    #p = GTFS_PATH
    #print(f'Exists: {p.exists()}')
    #print(f'Is dir: {p.is_dir()}')
    fetcher = GTFSFetcher()
    for filename, size in fetcher.list():
        print(f'{filename:30}  {size:10}')
