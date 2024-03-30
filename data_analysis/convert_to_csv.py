import os
import sys

import pandas as pd


if __name__ == "__main__":
    filename = sys.argv[1]
    name, ext = os.path.splitext(filename)
    if ext == '.json':
        csvfile = f'{name}.csv'
        df = pd.read_json(filename)
        df.to_csv(csvfile)
        os.remove(filename)
        print(f'Converted {filename} to {csvfile}')
