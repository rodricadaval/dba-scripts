#!/usr/bin/python3
import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.config import Config
import json
import sys
import logging
import time
import os
import signal
import pprint
import threading
import datetime


logging.basicConfig(level = logging.INFO)

AWS_PROFILE='xxx-xxx-east-1'
#STREAM_NAME='pg_kns_to_s3_consumer'
STREAM_NAME='binlog-backup'
S3_BUCKET='s3://binlog-backup'
LOGS='/var/log/X1X/backups'
BUCKET_NAME='binlog-backup'
RDS_NAME='microservicios-am'


s3 = None
file_name = None

session = boto3.Session(profile_name=AWS_PROFILE)
client = session.client('kinesis')

api_config = Config(
            region_name = "xx-east-1",
            connect_timeout = 5,
            read_timeout = 10,
            retries = {
                'max_attempts': 5,
                'mode': 'standard'
            }
        )
s3_client = session.client('s3', config=api_config)

aws_kinesis_stream = client.describe_stream(StreamName=STREAM_NAME)

shard_id = aws_kinesis_stream['StreamDescription']['Shards'][0]['ShardId']

stream_response = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType='TRIM_HORIZON'
)

iterator = stream_response['ShardIterator']


def signal_handler(signal, frame):
    s3.close()
    sys.exit(4)

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()


    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

def multi_part_upload_with_s3(file_path):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    t = S3Transfer(client=s3_client, config=config)

    key_path = '{}/{}'.format(RDS_NAME, os.path.basename(file_path))
    s3_client.upload_file(file_path, BUCKET_NAME, key_path, Config=config, Callback=ProgressPercentage(file_path))

def minutes_passed(oldepoch):
    #return time.time() - oldepoch >= 300
    return time.time() - oldepoch >= 300

def main():
  """
  Starts sending streams to S3
  """
  global iterator, s3

  oldepoch = time.time()

  utc_datetime = datetime.datetime.utcnow()
  timestr = utc_datetime.strftime("%Y%m%d-%H%M%S")
  #timestr = time.strftime("%Y%m%d-%H%M%S")

  global file_name
  file_name = "{}".format(timestr)
  file_path = os.path.dirname(__file__) + file_name

  while True:
      try:
        aws_kinesis_response = client.get_records(ShardIterator=iterator, Limit=5)
        iterator = aws_kinesis_response['NextShardIterator']
        for record in aws_kinesis_response['Records']:
            if 'Data' in record and len(record['Data']) > 0:
              #logging.info("Sending to S3 bucket: %s", json.loads(record['Data']))
              # aws S3 to upload the stream
              with open(file_path, "ab") as file_object:
                  # Append 'hello' at the end of file
                  file_object.write(record['Data'])
              if minutes_passed(oldepoch) and os.stat(file_path).st_size != 0:
                  multi_part_upload_with_s3(file_path)
                  oldepoch = time.time()
                  os.remove(file_path)
                  utc_datetime = datetime.datetime.utcnow()
                  timestr = utc_datetime.strftime("%Y%m%d-%H%M%S")
                  file_name = "{}".format(timestr)
                  file_path = os.path.dirname(__file__) + file_name


      except KeyboardInterrupt:
        signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    main()
