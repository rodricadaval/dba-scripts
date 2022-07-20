#!/usr/bin/python3

### max_wal_senders, max_replication_slots
### DROP created slots manually
### SELECT 'SELECT pg_drop_replication_slot('''||slot_name||''');'
### FROM pg_replication_slots WHERE plugin = 'wal2json';


import boto3
import json
import random
import calendar
import time
import os
import sys
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import argparse
import urllib
import logging
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
import threading
from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.config import Config
import datetime
import pprint
import portalocker

STREAM_NAME='binlog-backup'
S3_BUCKET='s3://binlog-backup'
LOGS='/var/log/X1X/backups'
BUCKET_NAME='binlog-backup'
s3 = None
file_name = None
file_path = None
oldepoch = None
file_instance = None

logging.basicConfig(level = logging.INFO)

def valid_profile(s):
    try:
        if s in ["xx-xx-east-1", "xx-xx-west-2", "xx1-xx-east-1", "xx1-xx-west-2", "xxx-xxx-east-1",
                        "xxx-xxx-west-2",
                     "xx1-xx-east-1", "xxxx-xx-west-2"]:
            return s
        else:
            raise ValueError(s)
    except ValueError:
        msg = "Not a valid profile: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(description='Stream changes to Kinesis')

parser.add_argument('-s', '--source', action="store", dest="engine_host", help="AWS Instance Endpoint",
                    required=True)
parser.add_argument('-d', '--database', action="store", dest="database", help="Database", required=False)
parser.add_argument('-u', '--user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Database Name",
                    required=False)
parser.add_argument('-a', '--action', action="store", dest="action", required=True)
parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | xx-xx-west-2 | xx1-xx-east-1 | "
                                                                      "xx1-xx-west-2 | xxx-xxx-east-1 | xxx-xxx-west-2 | "
                                                                      "xx1-xx-east-1 | xxxx-xx-west-2",
                    required=True, type=valid_profile)

args = parser.parse_args()

engine_host = args.engine_host
if args.database is not None:
    arg_db = args.database
    list_dbs = [x.strip() for x in arg_db.split(',')]
user = args.rep_user
if args.secret is not None:
    secret = args.secret
action = args.action
AWS_PROFILE = args.profile
AWS_REGION = AWS_PROFILE.split("-",1)[1]

api_config = Config(
    region_name=AWS_REGION,
    max_pool_connections=200,
    connect_timeout=5,
    read_timeout=300,
    retries={
        'max_attempts': 5,
        'mode': 'standard'
    }
)

session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3', config=api_config)
RDS_NAME = os.popen("nslookup " + engine_host + " | grep Name | cut -d: -f2 | xargs").readline().strip()

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

class FileObject:
    def __init__(self):
        self.file_name = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        self.file_path = os.path.dirname(__file__) + self.file_name
        self.file_start = time.time()
        self.uploading = False
        self.next = None

def multi_part_upload_with_s3(file_path):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    #t = S3Transfer(client=s3_client, config=config)
    key_path = 'pg/{}/{}'.format(RDS_NAME, os.path.basename(file_path))
    s3_client.upload_file(file_path, BUCKET_NAME, key_path, Config=config, Callback=ProgressPercentage(file_path))

def minutes_passed(aux_instance):
    return time.time() - aux_instance.file_start >= 120

def refresh_conn(db):
    my_connection = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'".format(db, engine_host, user, secret),
        connection_factory=LogicalReplicationConnection)
    return my_connection

def check_new_file(instance) -> FileObject:
    if instance.next is not None:
        if not instance.next.uploading:
            return instance.next
        else:
            instance.next.next = FileObject()
            return instance.next.next
    else:
        instance.next = FileObject()
        return instance.next


def write_and_send(consumer, msg, throttle=0.01):
    global file_instance
            
    try:
        if file_instance.uploading:
            aux_file_instance = check_new_file(file_instance)
        else:
            aux_file_instance = file_instance
        temp_bool = False
        if minutes_passed(aux_file_instance) and not aux_file_instance.uploading:
            aux_file_instance.uploading = True
            temp_bool = True
        file_object = open(aux_file_instance.file_path, "a")
        portalocker.lock(file_object, portalocker.LOCK_EX)
        node_data = str(msg.payload)
        payload = json.loads(node_data)
        payload['database'] = consumer.database
        file_object.write(json.dumps(payload) + "\n")
        portalocker.unlock(file_object)
        file_object.close()
        if temp_bool:
            multi_part_upload_with_s3(aux_file_instance.file_path)
            os.remove(aux_file_instance.file_path)
            if aux_file_instance.next is None:
                aux_file_instance.next = FileObject()
            file_instance = aux_file_instance.next
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
    except FileNotFoundError:
        time.sleep(0.01)
        write_and_send(consumer, msg, throttle*2)


def consume_data(consumer, cursor, conn, db):
    # print("Starting streaming, press Control-C to end...", file=sys.stderr)
    try:
        cursor.consume_stream(consumer, keepalive_interval=30)
    except KeyboardInterrupt:
        cursor.close()
        conn.close()
        print("The slot {0} still exists. Drop it with SELECT pg_drop_replication_slot({0}); if no longer needed."
              .format('wal2json_' + db.replace("-", "_") + '_slot'),
              file=sys.stderr)
        print("WARNING: Transaction logs will accumulate in pg_xlog "
              "until the slot is dropped.", file=sys.stderr)
    except (psycopg2.OperationalError, psycopg2.DatabaseError, psycopg2.errors.AdminShutdown) as e:
        logging.info('There was an error while consuming stream: {}. Trying to recover...'.format(str(e)))
        conn = refresh_conn(db)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}';"
                    .format('wal2json_' + db.replace("-", "_") + '_slot'))
        if cursor.fetchone() is not None:
            cursor.start_replication(slot_name='wal2json_' + db.replace("-", "_") + '_slot', decode=True)
            consume_data(consumer, cursor, conn, db)
        else:
            logging.info('Replication slot does not exists anymore: {}'
                         .format('wal2json_' + db.replace("-", "_") + '_slot'))
            sys.exit(4)


def start_stream(db):
    my_connection  = psycopg2.connect(
                       "dbname='{}' host='{}' user='{}' password='{}'".format(db, engine_host, user, secret) ,
                       connection_factory = LogicalReplicationConnection)
    cur = my_connection.cursor()
    cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}' and not active;"
                .format('wal2json_' + db.replace("-", "_") + '_slot'))

    try:
        cur.start_replication(slot_name='wal2json_' + db.replace("-", "_") + '_slot', decode=True, status_interval=5)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot('wal2json_' + db.replace("-", "_") + '_slot', output_plugin='wal2json')
        cur.start_replication(slot_name='wal2json_' + db.replace("-", "_") + '_slot', decode=True, status_interval=5)

    except psycopg2.errors.ObjectInUse:
        logging.info('No need to start replication slot. Already in use: {}'
                     .format('wal2json_' + db.replace("-", "_") + '_slot'))
        return "Done"
    except psycopg2.errors.AdminShutdown:
        logging.info('Admin user drop this thread in another process: {}'
                     .format('wal2json_' + db.replace("-", "_") + '_slot'))
        sys.exit(4)

    class DemoConsumer(object):
        def __init__(self):
            self.database = ""
            self.start = time.time()

        def __call__(self, msg):
            write_and_send(self, msg)

    democonsumer = DemoConsumer()
    democonsumer.database=db
    try:
        consume_data(democonsumer, cur, my_connection, db)
    except (psycopg2.OperationalError):
        start_stream(db)

def stop_stream(db):
    global secret
    my_connection  = psycopg2.connect(
                       "dbname='{}' host='{}' user='{}' password='{}'".format(db, engine_host, user, secret) ,
                       connection_factory = LogicalReplicationConnection)
    cur = my_connection.cursor()
    try:
        cur.execute("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{}';"
                    .format('wal2json_' + db.replace("-", "_") + '_slot'))
        cur.drop_replication_slot('wal2json_' + db.replace("-", "_") + '_slot')
        cur.close()
        my_connection.close()
    except psycopg2.ProgrammingError:
        print("There are no slots with this name {}".format('wal2json_' + db.replace("-", "_") + '_slot'))

    
def list_databases():
    global secret
    my_connection  = psycopg2.connect(
                       "dbname='{}' host='{}' user='{}' password='{}'".format("postgres", engine_host, user, secret))
    cur = my_connection.cursor()
    cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE datallowconn AND "
                "datname NOT IN ('{}',"
                "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                "'dba_%|%delete_me%' ORDER BY pg_database_size(datname) DESC LIMIT 150) as bla;".format('rdsadmin',
                                                                                                 'template1',
                                                                                    'template0',
                                                                       'postgres'))
    result = cur.fetchall()
    cur.close()
    my_connection.close()
    return result[0][0]

def check_secret():
    global secret
    if user == "dba_test_service":
        i_dynamo = dynamo.DynamoClient('dev')
        var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
        secret = var['value']

def define_global_vars():
    global file_instance
    file_instance = FileObject()

def main():
  """
  Starts streaming and sending to s3 after x minutes
  """
  define_global_vars()
  check_secret()

  if action in ["start","stop"]:
      # create a list of threads
      threads = []
      if "arg_db" in globals():
          dbs = list_dbs
      else:
          dbs = list_databases()
      # In this case 'dbs' is a list of databases that discarding internal and delete_me waste.
      for i in dbs:
          # We start one thread per database present.
          if action == "start":
              process = threading.Thread(target=start_stream, args=[i.replace("\"","")], name=i.replace("\"", ""))
          elif action == "stop":
              process = threading.Thread(target=stop_stream, args=[i.replace("\"", "")], name=i.replace("\"", ""))
          else:
              raise NotImplementedError("No action implemented with this name {}".format(action))
          process.daemon = True
          process.start()
          logging.info('Opening thread name:{}, status:{}, id:{}'.format(process.getName(), process.is_alive(),
                                                                   process.ident))
          threads.append(process)
      # We now pause execution on the main thread by 'joining' all of our started threads.
      # This ensures that each has finished processing the databases.
      for process in threads:
          try:
              # At this point, results for each DATABASE are being stored in a temporal file that acts like a buffer.
              process.join()
          except KeyboardInterrupt:
              logging.info('Exiting all processes due to SIGTERM'.format())
              sys.exit(4)
          except Exception as e:
              print(e)
              sys.exit(4)



  else:
        print("There are no actions with this name")

if __name__ == '__main__':
    main()