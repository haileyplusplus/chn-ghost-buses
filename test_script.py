import boto3
import sys

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

print("Printing bucket objects:")
print(client.list_objects_v2(
    Bucket='chn-ghost-buses-public',
    Prefix='bus_full_day_data_v2/'
))

s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)
content="String content to write to a new S3 file"
s3.Object('chn-ghost-buses-public', 'newfile.txt').put(Body=content)