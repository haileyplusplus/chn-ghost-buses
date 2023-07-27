import boto3
import sys
import data_analysis.static_gtfs_analysis as sga
import pendulum
from io import StringIO

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

date = pendulum.now().to_date_string()

zipfile, zipfile_bytes_io = sga.download_cta_zip()
print(f'Saving zipfile available at '
      f'https://www.transitchicago.com/downloads/sch_data/google_transit.zip '
      f'on {date} to public bucket')
zipfile_bytes_io.seek(0)
client.upload_fileobj(zipfile_bytes_io, 'chn-ghost-buses-public', f'google_transit_{date}.zip')

data = sga.download_extract_format()
trip_summary = sga.make_trip_summary(data)

route_daily_summary = (
    sga.summarize_date_rt(trip_summary)
)

csv_buffer = StringIO()
route_daily_summary.to_csv(csv_buffer)

print(f'Saving cta_route_daily_summary_{date}.csv to public bucket')
s3.Object('chn-ghost-buses-public', f'cta_route_daily_summary_{date}.csv')\
    .put(Body=csv_buffer.getvalue())


# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
print('Confirm that objects exist in bucket')
s3_paginator = client.get_paginator('list_objects_v2')

def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            if content['Key'] in [f'cta_route_daily_summary_{date}.csv', f'google_transit_{date}.zip']:
                yield f"{content['Key']} exists"

keys('chn-ghost-buses-public')