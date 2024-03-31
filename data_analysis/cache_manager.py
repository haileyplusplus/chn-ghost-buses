import os
from pathlib import Path

import logging
import datetime

import pandas
import pandas as pd
import requests
from io import BytesIO

BUCKET = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
DATA_DIR = Path(__file__).parent.parent / "data_output" / "scratch"


#IGNORE = '20230211'
#IGNORE = '20221020'
IGNORE = '20231216'
#IGNORE = 's'

class CacheManager:
    def __init__(self, subdir):
        self.cache_dir = DATA_DIR / subdir
        if not self.cache_dir.exists():
            self.cache_dir.mkdir()

    def retrieve(self, filename: str, url: str) -> BytesIO:
        filepath = self.cache_dir / filename
        if filepath.exists():
            logging.info(f'Retrieved cached {url} from {filename}')
            return BytesIO(filepath.open('rb').read())
        bytes_io = BytesIO(requests.get(url).content)
        with filepath.open('wb') as ofh:
            ofh.write(bytes_io.getvalue())
        logging.info(f'Stored cached {url} in {filename}')
        return bytes_io

    @staticmethod
    def fix_dt_column(df, c):
        def fixer(x):
            if type(x) is not int:
                return pd.NaT
            return datetime.datetime.fromtimestamp(x / 1000).astimezone(datetime.UTC)
        df[c] = df[c].apply(fixer)
        return df

    def retrieve_calculated_dataframe(self, filename, func, dt_fields: list[str]) -> pd.DataFrame:
        filepath = self.cache_dir / filename
        csv = filename.endswith('.csv')
        if filename.replace('-', '').startswith(IGNORE):
            print(f'Ignoring whether {filename} is in cache')
            return func()
        if csv and not filepath.exists():
            print(f'Using csv fallback for {filename}')
            csv = False
            filepath = self.cache_dir / filename.replace('.csv', '.json')
        if filepath.exists():
            logging.info(f'Retrieved {filename} from cache')
            if csv:
                logging.debug(f'Reading csv from {filepath}')
                df = pd.read_csv(filepath)
            else:
                df = pd.read_json(filepath)
            assert type(df) is pd.DataFrame
            if df.empty:
                return pd.DataFrame()
            for c in dt_fields:
                df = self.fix_dt_column(df, c)
            #print('Retrieved df')
            #print(df)
            return df
        logging.info(f'Writing {filename} to cache')
        df = func()
        if csv:
            df.to_csv(filepath)
        else:
            df.to_json(filepath)
        #print(f'Storing df')
        #print(df)
        return df
