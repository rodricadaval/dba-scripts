#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform replace table to new one reducing archiving old rows and converting columns to biginteger.

import os
import requests
import json
import datetime
import psycopg2
import re
from pprint import pprint
import sys
import argparse
import time
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
sys.path.insert(0, git_root + '/mongodb/common')
from lib import dynamo
from lib import dynamo_rds
from lib.settings import Settings as global_settings
from lib import cloudwatch
import logging
from logging.handlers import TimedRotatingFileHandler
import multiprocessing

FORMATTER = logging.Formatter("%(asctime)s --- %(message)s")
LOG_FILE = "pg-schema-change.log"
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

version = "0.1.9"

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   console_handler.setLevel(logging.WARNING)
   return console_handler

def get_file_handler():
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   file_handler.setLevel(logging.WARNING)
   return file_handler

def custom_logger():
   logger = multiprocessing.get_logger()
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger

my_logger = custom_logger()

parser = argparse.ArgumentParser(description='Replacing table with another')
parser.add_argument('-v', '--version', action='version', version='%(prog)s version {}'.format(version))
parser.add_argument('--host', action="store", dest="db_host", help="Rds Endpoint",
                    required=True)
parser.add_argument('-d', '--database', action="store", dest="db", help="Database", required=True)
parser.add_argument('-s', '--schema', action="store", dest="db_schema", default="public",
                    help="Table Schema")
parser.add_argument('-t', '--table', action="store", dest="db_table", help="Table Name",
                    required=True)
parser.add_argument('-c', '--column', action="store", dest="db_column", help="Table column to alter",
                    required=True)
parser.add_argument('-u', '--user', action="store", dest="db_user", default="dba_test_service", help="User",
                    required=False)
parser.add_argument('-p', '--password', action="store", dest="db_secret", help="User Password",
                    required=False)
parser.add_argument('-q', '--quantity-of-rpb', action="store", dest="step", default=200, help="Batches - (default: %(default)s)",
                    required=False)
parser.add_argument('-b', '--begin-with', action="store", dest="begin_with", help="Begin in value",
                    required=False)
parser.add_argument('-j', '--jobs', action="store", dest="jobs", default=2, help="Worker jobs to use - (default: %(default)s)",
                    required=False)
parser.add_argument('--force-max-cpu', action="store", dest="max_cpu", default=60, help="Worker jobs to use - (default: %(default)s)",
                    required=False)
parser.add_argument('--force-jobs', action="store_true", dest="force_jobs", help="Force worker jobs to use for "
                                                                                     "dates",
                    required=False)
parser.add_argument('--force-max-diskqueue', action="store", dest="max_disk_queue", default=30, help="Max Disk Queue - (default: %(default)s)",
                    required=False)
parser.add_argument('--force-max-replication-slot-lag', action="store", dest="max_replication_slot_lag", default=100, help="Max Oldest Replication Slot Lag in GigaBytes - (default: %(default)s GigaBytes)",
                    required=False)                    
parser.add_argument('--force-max-rl', action="store", dest="max_read_latency", default=0.02, help="Max Read Latency - (default: %(default)s)",
                    required=False)
parser.add_argument('--force-max-wl', action="store", dest="max_write_latency", default=0.08, help="Max Write Latency - (default: %(default)s)",
                    required=False)
parser.add_argument('--force-coned-table-name', action="store", dest="force_cloned_table_name", help="Do not use '_cloned' suffix for cloned table. Specify a table name",
                    required=False)
parser.add_argument('--no-limits', action="store_true", dest="no_limits", help="Do not check RDS metrics",
                    required=False)
parser.add_argument('--force-continue', action="store_true", dest="force_continue", help="Skip foreign key check",
                    required=False)
parser.add_argument('--no-fks', action="store_true", dest="no_fks", help="Skip foreign keys creation in cloned",
                    required=False)
parser.add_argument('--force-drop', action="store_true", dest="force_drop", help="Force dropping table",
                    required=False)
parser.add_argument('--force-fivetran-swap-ok', action="store_true", dest="force_fivetran_swap_ok", help="Fivetran needs doing some changes before the rename process. If it's ok, you can force the rename",
                    required=False)
parser.add_argument('--speed', action="store", dest="speed", default="0.2", help="Time to wait per worker operation",
                    required=False)
parser.add_argument('-e', '--end-with', action="store", dest="end_with", help="End with value",
                    required=False)
parser.add_argument('-a', '--action', action="store", dest="action",
                    help="Action -> (start, create-new-table, show-create-table, create-trigger, "
                         "check-instance-metrics, compare-count, drop-coned-table)",
                    required=True)

args = parser.parse_args()

db_host = args.db_host
db_user = args.db_user
db = args.db
db_schema = args.db_schema
db_table = args.db_table
if args.force_cloned_table_name:
    db_table_cloned = args.force_cloned_table_name
else:
    db_table_cloned = db_table + "_cloned"
db_column = args.db_column
if args.begin_with:
    begin_with = args.begin_with
else:
    begin_with = 1
if args.end_with:
    max_value = args.end_with
else:
    max_value = 0
speed = float(args.speed)
step = int(args.step)
action = args.action
logs = []
handler = []
manager = None
date_format = '%Y-%m-%d'
time_format = "%H:%M:%S.%f" if '.' in str(begin_with) else "%H:%M:%S"
pattern = '{} {}'.format(date_format, time_format)
date_begin_with = ""
date_end_with = ""

__tg_name = 'tg_cloning_table_{}_{}'.format(db_schema.replace("-", "_"), db_table.replace("-", "_"))
__proc_name = '\"{}\".f_cloning_table_{}_{}'.format(db_schema, db_schema.replace("-", "_"), db_table.replace(
    "-","_"))
__qry_last_value = 'SELECT max("{}") FROM "{}"."{}" WHERE "{}" <= '.format(db_column, db_schema, db_table_cloned, db_column)
__qry_master_last_value = 'SELECT max("{}") FROM "{}"."{}";'.format(db_column, db_schema, db_table)
__qry_master_min_value = 'SELECT min("{}") FROM "{}"."{}";'.format(db_column, db_schema, db_table)
__qry_create_trigger = """
CREATE TRIGGER {} AFTER INSERT OR DELETE OR UPDATE ON \"{}\".\"{}\" 
FOR EACH ROW EXECUTE PROCEDURE {}()
""".format(__tg_name, db_schema, db_table, __proc_name)
__check_tg = """
SELECT event_manipulation, action_statement, action_orientation, action_timing
FROM information_schema.triggers
WHERE event_object_table = '{}'
AND event_object_schema = '{}'
AND trigger_name = '{}';
""".format(db_table, db_schema, __tg_name)
__check_other_tg = """
SELECT trigger_name, event_manipulation, action_statement, action_orientation, action_timing
FROM information_schema.triggers
WHERE event_object_table = '{}'
AND event_object_schema = '{}'
AND trigger_name != '{}';
""".format(db_table, db_schema, __tg_name)
__drop_tg = 'DROP TRIGGER {} ON \"{}\".\"{}\";'.format(__tg_name, db_schema, db_table)
__drop_tg_old = 'DROP TRIGGER {} ON \"{}\".\"{}_old\";'.format(__tg_name, db_schema, db_table)
__drop_proc = 'DROP FUNCTION IF EXISTS {}();'.format(__proc_name)
__qry_check_referencing_tables = """
select distinct 	  
fk_tco.table_schema as fk_schema_name,
fk_tco.table_name as fk_table_name,
fk_tco.constraint_name as fk_name
from information_schema.referential_constraints rco
join information_schema.table_constraints fk_tco on rco.constraint_name = fk_tco.constraint_name and rco.constraint_schema = fk_tco.table_schema
join information_schema.table_constraints pk_tco on rco.unique_constraint_name = pk_tco.constraint_name and rco.unique_constraint_schema = pk_tco.table_schema
where pk_tco.table_name = '{}' and pk_tco.table_schema = '{}' order by fk_table_name;
""".format(db_table, db_schema)
__qry_sequences = """
SELECT relname as seq_name, a.attname as column_name FROM pg_depend d JOIN pg_attribute a ON a.attrelid = d.refobjid 
AND a.attnum = d.refobjsubid JOIN pg_class r ON r.oid = objid JOIN pg_namespace n ON n.oid = relnamespace WHERE 
d.refobjsubid > 0 AND relkind = 'S' AND nspname = '{}' AND d.refobjid::regclass::text = '{}';"""\
    .format(db_schema,db_schema + "." + db_table if str(db_schema) != "public" else db_table)
__qry_sequences_old = """
SELECT relname as seq_name, a.attname as column_name FROM pg_depend d JOIN pg_attribute a ON a.attrelid = d.refobjid 
AND a.attnum = d.refobjsubid JOIN pg_class r ON r.oid = objid JOIN pg_namespace n ON n.oid = relnamespace WHERE 
d.refobjsubid > 0 AND relkind = 'S' AND nspname = '{}' AND d.refobjid::regclass::text = '{}';"""\
    .format(db_schema,db_schema + "." + db_table + "_old" if str(db_schema) != "public" else db_table + "_old")
__qry_get_in_table_fks = """
SELECT
tc.table_schema,
tc.constraint_name, 
tc.table_name,     
ccu.table_schema AS foreign_table_schema,
ccu.table_name AS foreign_table_name,
string_agg(distinct quote_ident(kcu.column_name), ',') as original_tab_columns, 
string_agg(distinct quote_ident(ccu.column_name), ',') AS foreign_column_names
FROM 
information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
ON tc.constraint_name = kcu.constraint_name
AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
ON ccu.constraint_name = tc.constraint_name
AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' 
AND tc.table_schema='{}' 
AND tc.table_name='{}'
GROUP BY 1,2,3,4,5;
""".format(db_schema, db_table)

__qry_column_check_indexes = """
SELECT
schema_name,
table_name,
index_name,
string_agg(column_name, ',')
FROM (
SELECT
    t.relnamespace::regnamespace::text as schema_name,
    t.relname AS table_name,
    i.relname AS index_name,
    a.attname AS column_name,
    (SELECT i
    FROM (SELECT
            *,
            row_number()
            OVER () i
            FROM unnest(indkey) WITH ORDINALITY AS a(v)) a
    WHERE v = attnum)
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
WHERE
    t.oid = ix.indrelid
    AND i.oid = ix.indexrelid
    AND a.attrelid = t.oid
    AND a.attnum = ANY (ix.indkey)
    AND t.relkind IN ('r','p')
    AND t.relname = '%s'
    AND t.relnamespace::regnamespace::text = '%s'
ORDER BY table_name, index_name, i
) raw
GROUP BY schema_name, table_name, index_name;
"""

__qry_check_copies_or_vacuums = """
SELECT pid, usename, datname, query FROM pg_stat_activity 
WHERE (query ilike '%vacuum%{0}%{1}' 
OR query ilike '%vacuum%{0}%{1}"%' 
OR query ilike '%vacuum%{0}%{1} %'
OR query ilike '%vacuum%{0}%{1};'
OR query like 'COPY%FROM%{0}%{1}' 
OR query like 'COPY%FROM%{0}%{1};' 
OR query like 'COPY%FROM%{0}%{1} %' 
OR query like 'COPY%FROM%{0}%{1}"%')
AND pid <> pg_backend_pid();
""".format(db_schema, db_table)

def search_instance_in_dynamo(instance, region):
    for country in global_settings.AWS_PROFILE:
        if region in global_settings.AWS_PROFILE[country]:
            dynamordscli = dynamo_rds.DynamoRDSClient(str(country).lower())
            item = dynamordscli.get_vars(instance)
            if item:
                return json.loads(item['value'])
    return None

def get_country(instance, region):
    instance_data = search_instance_in_dynamo(instance, region)
    return instance_data['inventory']['country']

def open_conn():
    conn = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'"
            .format(db, db_host , db_user, secret))
    conn.autocommit = True
    return conn

def open_uncommited_conn():
    conn = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'"
            .format(db, db_host , db_user, secret))
    conn.autocommit = False
    return conn

def huge_table(conn, analyzed = False):
    with conn.cursor() as cursor:
        cursor.execute("SELECT ut.n_live_tup FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                       "'{}';".format(db_schema, db_table))
        result = cursor.fetchone()
        if result is not None:
            if int(result[0]) == 0 and not analyzed:
                cursor.execute(
                    "ANALYZE \"{}\".\"{}\";".format(db_schema, db_table))
                return huge_table(conn, True)
            elif int(result[0]) > 10000000:
                return True
    return False

def check_column_has_index():
    try:
        my_logger.warning("Checking if column %s has index" % (db_column))
        with open_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(__qry_column_check_indexes % (db_table, db_schema))
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        if row[3].split(',')[0] == db_column:
                            my_logger.warning("...OK")
                            return True
                    return False
                else:
                    my_logger.warning("There is no indexes created using the column %s" % (db_column))
                    return False
    except (Exception, psycopg2.DatabaseError) as error:
        my_logger.error(error)
        sys.exit(6)

def is_partition_parent(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT c.relname FROM pg_class AS c WHERE c.relkind IN ('p') "
                       "AND c.relnamespace::regnamespace::text = '{}' and relname = '{}';".format(db_schema, db_table))
        result = cursor.fetchone()
        return result

def get_table_estimated_rows(conn, analyzed = False):
    with conn.cursor() as cursor:
        if is_partition_parent(conn):
            acctuples = 0
            cursor.execute(
                "SELECT schemaname, relname, last_analyze > now() - interval '3 days' FROM pg_stat_user_tables ut WHERE schemaname "
                "= '{}' AND relname ilike '{}%';".format(db_schema, db_table))
            result = cursor.fetchall()
            if result:
                for row in result:
                    if not row[2]:
                        my_logger.warning("Performing analyze in {} due to reason: \"last one is older than 3 days\""
                                          .format(str(row[1])))
                        cursor.execute(
                            "ANALYZE \"{}\".\"{}\";".format(str(row[0]), str(row[1])))
                    cursor.execute(
                        "SELECT ut.n_live_tup FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                        "'{}';".format(str(row[0]), str(row[1])))
                    estimated_tuples = cursor.fetchone()
                    if estimated_tuples:
                        if int(estimated_tuples[0]) == 0 and not analyzed:
                            cursor.execute(
                                "ANALYZE \"{}\".\"{}\";".format(str(row[0]), str(row[1])))
                            return get_table_estimated_rows(conn, True)
                        else:
                             acctuples = acctuples + int(estimated_tuples[0])
                return acctuples
            return get_table_estimated_rows(conn, True)
        else:
            cursor.execute("SELECT last_analyze > now() - interval '3 days' FROM pg_stat_user_tables ut "
                           "WHERE schemaname = '{}' AND relname = '{}';".format(db_schema, db_table))
            result = cursor.fetchone()
            if not result:
                my_logger.warning("Performing analyze due to reason: \"last one is older than 3 days\"")
                cursor.execute(
                    "ANALYZE \"{}\".\"{}\";".format(db_schema, db_table))
                return get_table_estimated_rows(conn, True)
            cursor.execute("SELECT ut.n_live_tup FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                           "'{}';".format(db_schema, db_table))
            result = cursor.fetchone()
            if result:
                if int(result[0]) == 0 and not analyzed:
                    cursor.execute(
                        "ANALYZE \"{}\".\"{}\";".format(db_schema, db_table))
                    return get_table_estimated_rows(conn, True)
                else:
                    return result[0]
        return None

def get_master_max_value(connection) -> int:
    global pattern
    try:
        if max_value != 0:
            return max_value
        with connection.cursor() as cur:
            cur.execute(__qry_master_last_value)
            result = cur.fetchone()
        if result[0] is not None:
            time_var = None
            res = result[0]
            if str(identity_column_type) not in ["integer", "bigint", "date"]:
                if "." in str(result[0]):
                    res = str(result[0])
                    pattern = "{} %H:%M:%S.%f".format(date_format)
                    if "with time zone" in str(identity_column_type):
                        if isinstance(res, str):
                            size = len(res)
                            if ":" in res[:size - 3] and "-" in res[:size - 6]:
                                res = res[:size - 6]
                            elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                                res = res[:size - 3]
                            time_var = time.mktime(time.strptime(str(res), pattern))
                        else:
                            time_var = time.mktime(time.strptime(str(res.utcnow()), pattern))
                    else:
                        time_var = time.mktime(time.strptime(str(res), pattern))
                elif "." not in str(result[0]):
                    res = str(result[0])
                    pattern = "{} %H:%M:%S".format(date_format)
                    if "with time zone" in str(identity_column_type):
                        if isinstance(res, str):
                            size = len(res)
                            if ":" in res[:size - 3] and "-" in res[:size - 6]:
                                res = res[:size - 6]
                            elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                                res = res[:size - 3]
                            time_var = time.mktime(time.strptime(str(res), pattern))
                        else:
                            time_var = time.mktime(time.strptime(str(res.utcnow()), pattern))
                    else:
                        time_var = time.mktime(time.strptime(str(res), pattern))
                else:
                    time_var = time.mktime(time.strptime(str(res), pattern))
                return int(time_var)
            else:
                return res
        else:
            raise psycopg2.ProgrammingError("Error al consultar el max column value: {}".format(str(result[0])))
    except psycopg2.OperationalError as e:
        my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def get_master_date_max_value(connection) -> str:
    try:
        with connection.cursor() as cur:
            cur.execute(__qry_master_last_value)
            result = cur.fetchone()
        if result[0] is not None:
            res = str(result[0])
            if "with time zone" in str(identity_column_type):
                size = len(str(res))
                if ":" in res[:size - 3] and "-" in res[:size - 6]:
                    res = res[:size - 6]
                elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                    res = res[:size - 3]
            return res
        else:
            raise psycopg2.ProgrammingError("Error al consultar el max column value: {}".format(str(result[0])))
    except psycopg2.OperationalError as e:
        my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def get_master_date_min_value(connection) -> str:
    try:
        with connection.cursor() as cur:
            cur.execute(__qry_master_min_value)
            result = cur.fetchone()
        if result[0] is not None:
            res = str(result[0])
            if "with time zone" in str(identity_column_type):
                size = len(str(res))
                if ":" in res[:size - 3] and "-" in res[:size - 6]:
                    res = res[:size - 6]
                elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                    res = res[:size - 3]
            return res
        else:
            raise psycopg2.ProgrammingError("Error al consultar el min column value: {}".format(str(result[0])))
    except psycopg2.OperationalError as e:
        my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def get_master_min_value(connection) -> int:
    global pattern
    try:
        with connection.cursor() as cur:
            cur.execute(__qry_master_min_value)
            result = cur.fetchone()
        if result[0] is not None:
            time_var = None
            res = result[0]
            if str(identity_column_type) not in ["integer", "bigint", "date"]:
                if "." in str(result[0]):
                    res = str(result[0])
                    pattern = "{} %H:%M:%S.%f".format(date_format)
                    if "with time zone" in str(identity_column_type):
                        if isinstance(res, str):
                            size = len(res)
                            if ":" in res[:size - 3] and "-" in res[:size - 6]:
                                res = res[:size - 6]
                            elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                                res = res[:size - 3]
                            time_var = time.mktime(time.strptime(str(res), pattern))
                        else:
                            time_var = time.mktime(time.strptime(str(res.utcnow()), pattern))
                    else:
                        time_var = time.mktime(time.strptime(str(res), pattern))
                    return int(time_var)
                elif "." not in str(result[0]):
                    res = str(result[0])
                    pattern = "{} %H:%M:%S".format(date_format)
                    if "with time zone" in str(identity_column_type):
                        if isinstance(res, str):
                            size = len(res)
                            if ":" in res[:size - 3] and "-" in res[:size - 6]:
                                res = res[:size - 6]
                            elif ":" not in res[:size - 3] and "-" in res[:size - 3]:
                                res = res[:size - 3]
                            time_var = time.mktime(time.strptime(str(res), pattern))
                        else:
                            time_var = time.mktime(time.strptime(str(res.utcnow()), pattern))
                    else:
                        time_var = time.mktime(time.strptime(str(res), pattern))
                else:
                    time_var = time.mktime(time.strptime(str(res), pattern))
                return int(time_var)
            else:
                return res
        else:
            raise psycopg2.ProgrammingError("Error al consultar el min column value: {}".format(str(result[0])))
    except psycopg2.OperationalError as e:
        my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def get_identity_column_type():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT table_schema, table_name, column_name, data_type FROM "
                               "information_schema.columns WHERE "
                               "table_schema = '{}' "
                               "AND table_name = '{}' AND column_name = '{}';".format(db_schema,db_table, db_column))
                result = cursor.fetchone()
                if result:
                   return result[3]
                else:
                    raise TypeError
    except TypeError as e:
        my_logger.error("No existe identity column con los parametros enviados: {}".format(e))
        sys.exit(500)
    except Exception as e:
        my_logger.error("Error al chequear tipo de identity column: {}".format(e))
        sys.exit(500)

def check_secret():
    if db_user == "dba_test_service":
        try:
            i_dynamo = dynamo.DynamoClient('dev')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']
        except Exception as e:
            print("No se encontraron variables del user en dynamo dev, busco en prod")
            i_dynamo = dynamo.DynamoClient('xx')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']

if args.db_secret is not None:
    secret = args.db_secret
else:
    secret = check_secret()
identity_column_type = get_identity_column_type()

class Payload(object):
    endpoint = ""
    region = ""
    instance = ""
    country = ""
    # The class "constructor" - It's actually an initializer
    def __init__(self):
        self.endpoint = os.popen("nslookup " + db_host + " | grep Name | cut -d: -f2 | xargs").readline().strip()
        self.region = self.endpoint.split(".")[2]
        self.instance = self.endpoint.split(".")[0]
        self.country = get_country(self.instance, self.region)
        self.max_cpu = float(args.max_cpu)
        self.max_disk_queue = float(args.max_disk_queue)
        self.max_read_latency = float(args.max_read_latency)
        self.max_write_latency = float(args.max_write_latency)
        self.max_replication_slot_lag = float(args.max_replication_slot_lag)
        self.max_value = get_master_max_value(open_conn())
        if "timestamp" in str(identity_column_type) or "date" in str(identity_column_type):
            self.max_value_formatted = to_timestamp(self.max_value)
        else:
            self.max_value_formatted = None
        self.db_host = db_host
        self.db = db
        self.db_user = db_user
        self.db_secret = secret

def to_timestamp(epoch):
    mytimestamp = datetime.datetime.fromtimestamp(epoch)
    return mytimestamp.strftime(pattern)

def to_epoch(timestamp):
    return int(time.mktime(time.strptime(timestamp, pattern)))

def set_steps(epoch_begin, epoch_end):
    with open_conn() as connection:
        rows = get_table_estimated_rows(connection, False)
    if identity_column_type == "date":
        return 86400
    else:
        if round((rows/(epoch_end - epoch_begin)), 0) > 10000:
            return 15
        if rows > 1000000000:
            val = 30
        elif rows > 500000000:
            val = 60
        elif rows > 100000000:
            val = 120
        elif rows > 50000000:
            val = 240
        elif rows > 10000000:
            val = 1000
        else:
            return 43200
        return int(round(((rows/(epoch_end - epoch_begin)) * val),0))

def fill_extra_data():
    global jobs, max_value, secret, pattern, begin_with, time_format, identity_column_type

    jobs = int(args.jobs)

    if identity_column_type not in ["bigint", "integer"]:
        if "timestamp(0)" in identity_column_type:
            time_format = '%H:%M:%S'
        elif "timestamp(" in identity_column_type:
            time_format = '%H:%M:%S.%f'
        elif "timestamp with" in identity_column_type:
            time_format = '%H:%M:%S'
        elif identity_column_type == "date":
            time_format = None
        pattern = date_format + ' ' + time_format if time_format else date_format
        if time_format:
            if " " not in str(begin_with) and time_format:
                begin_with = str(begin_with) + " 00:00:00"
            if "." not in begin_with and "." in time_format:
                begin_with = str(begin_with) + ".000"
        else:
            begin_with = str(begin_with)
        if args.end_with:
            if time_format:
                if " " not in args.end_with and time_format:
                    max_value = str(args.end_with) + " 00:00:00"
                if "." not in args.end_with and "." in time_format:
                    max_value = str(max_value) + ".000"
            else:
                max_value = str(max_value)

def resolve_begin_and_steps():
    global begin_with, date_begin_with, jobs, step, max_value, date_end_with
    if "timestamp" in str(identity_column_type) or "date" in str(identity_column_type):
        if args.begin_with:
            begin_with = to_epoch(begin_with)
        else:
            with open_conn() as conn:
                begin_with = get_master_min_value(conn)
        if args.end_with:
            max_value = to_epoch(max_value)
        else:
            with open_conn() as conn:
                max_value = get_master_max_value(conn)
        step = set_steps(begin_with, max_value)
        if not args.force_jobs:
            jobs = 1
        mytimestamp = datetime.datetime.fromtimestamp(begin_with)
        date_begin_with = mytimestamp.strftime(pattern)
        mytimestamp = datetime.datetime.fromtimestamp(max_value)
        date_end_with = mytimestamp.strftime(pattern)
    elif str(identity_column_type) in ["bigint", "integer"]:
        if not args.begin_with:
            with open_conn() as conn:
                begin_with = get_master_min_value(conn)
    else:
        my_logger.warning("Eligió la columna: {} del tipo {}. No es un tipo de dato valido".format(db_column,
                                                                                                   identity_column_type))
        sys.exit(1)

fill_extra_data()
resolve_begin_and_steps()


def get_slack_url():
    try:
        i_dynamo = dynamo.DynamoClient('dev')
        var = json.loads(i_dynamo.get_vars('dba_slack_url_hook'))
        return var['value']
    except Exception as e:
        print("No se encontraron variables de dba slack en dynamo dev, busco en prod")
        i_dynamo = dynamo.DynamoClient('xx')
        var = json.loads(i_dynamo.get_vars('dba_slack_url_hook'))
        return var['value']

slackUrl = get_slack_url()
slackChannel = "dba_boto3_reports"

def sendToSlack(var_data):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", \
            "author_name": "Alert sequences reaching max integer value", \
            "fields": [ \
            {"title": "Schema.table.column", "value": "' + str(var_data[0]) + '", "short": true}, \
            {"title": "Last value", "value": "' + str(var_data[1]) + '","short": true}, \
            {"title": "Increments by","value": "' + str(var_data[2]) + '","short": true}, \
            {"title": "Database","value": "' + str(var_data[4]).replace("\"","") + '","short": true}, \
            {"title": "Remaining ids","value": "' + str(var_data[3]) + '","short": true}, \
            {"title": "RDS","value": "' + str(var_data[5]) + '","short": true}, \
            {"title": "AWS Account","value": "' + str(fullprofile) + '","short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last check", \
            "footer": "last check", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-RDS-integer-limit", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)

def get_cloned_max_value(connection) -> int:
    try:
        with connection.cursor() as cur:
            cur.execute(__qry_last_value + '{};'.format(max_value))
            result = cur.fetchone()
        if result is not None:
            return result[0]
        else:
            raise psycopg2.ProgrammingError("Error al consultar el max column value")
    except psycopg2.OperationalError as e:
        my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def replace_with_nines(match_obj):
    if match_obj.group(1) is not None:
        return match_obj.group(1) + match_obj.group(2).replace("0","9")

def execute_iteration(alogger, queue, latest_bigtable_id, workers, event, shared_dict, job_id):
    try:
        connection = open_conn()
        counter = int(queue)
        while counter <= latest_bigtable_id:
            event.wait()
            with connection.cursor() as cursor:
                if counter + step > latest_bigtable_id:
                    max_val = latest_bigtable_id
                else:
                    max_val = counter + step
                if "timestamp" in shared_dict["type"] or "date" in shared_dict["type"]:
                    if begin_with == counter and not args.begin_with:
                        counter_aux = get_master_date_min_value(connection)
                    else:
                        counter_aux = to_timestamp(counter)
                    if counter + step > latest_bigtable_id and not args.end_with:
                        max_val_aux = get_master_date_max_value(connection)
                    else:
                        max_val_aux = to_timestamp(max_val)
                        if "." in str(max_val_aux):
                            max_val_aux = re.sub(r"(.*)(\.[0-9]+)", replace_with_nines, max_val_aux)
                else:
                    counter_aux = counter
                    max_val_aux = max_val
                if "timestamp" in shared_dict["type"] or "date" in shared_dict["type"]:
                    cursor.execute('INSERT INTO "{0}"."{1}" SELECT * FROM "{0}"."{2}" WHERE "{3}" BETWEEN \'{4}\' AND '
                                   '\'{5}\' ON CONFLICT DO NOTHING;'
                                   .format(db_schema, db_table_cloned, db_table, db_column, counter_aux, max_val_aux))
                else:
                    cursor.execute(
                        'INSERT INTO "{0}"."{1}" SELECT * FROM "{0}"."{2}" WHERE "{3}" BETWEEN {4} AND {5} ON '
                        'CONFLICT DO NOTHING;'
                        .format(db_schema, db_table_cloned, db_table, db_column, counter_aux, max_val_aux))
                shared_dict['worker-'+str(job_id+1)]['value'] = max_val
            counter = counter + (step * workers) + workers
            time.sleep(shared_dict['wait_per_transaction'])
        shared_dict['worker-' + str(job_id + 1)]['finished'] = True
        connection.close()
    except Exception as e:
        alogger.error(e)

def populate_cloned_table(counter):
    global dictio
    latest_bigtable_id = max_value
    dictio['type'] = identity_column_type
    if identity_column_type in ['bigint', 'integer'] and isinstance(counter, str):
        counter = int(counter)
    for i in range(jobs):
        queue.put(counter+i+(i*step))
        dictio['worker-'+str(i+1)] = manager.dict({"value":0,"finished":False})
        if args.no_limits:
            dictio['wait_per_transaction'] = 0.00001
            dictio['NoLimits'] = True
        else:
            dictio['wait_per_transaction'] = speed
            dictio['NoLimits'] = False

    start_time = datetime.datetime.now()
    my_logger.warning("Start time: %s" % (start_time,))

    validate_rds_metrics(event, my_logger, dictio, False)

    processes = [multiprocessing.Process(target=execute_iteration, args=(my_logger, queue.get(), latest_bigtable_id,
                                                                         jobs,
                                                                         event, dictio,
                                                            _)) for
                 _ in
                 range(jobs)]

    processes.append(multiprocessing.Process(target=validate_rds_metrics, args=(event,
                                                                                my_logger, dictio)))

    for p in processes:
        p.daemon = True
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        aux = latest_bigtable_id
        message = dict()
        for key in dictio.keys():
            if "worker" in key:
                if dictio[key]['value'] < aux:
                    aux = dictio[key]['value']
                message[key] = to_timestamp(dictio[key]['value']) if "timestamp" in str(dictio['type']) or "date" \
                                                       in str(dictio['type']) else dictio[key]['value']
        my_logger.warning("Cancelled with stats: {}".format(message))
        my_logger.warning("If continue, value {} is suggested".format(to_timestamp(aux)
                                                                      if "timestamp" in str(dictio['type']) or "date"
                                                                         in str(dictio['type']) else aux))
        sys.exit(1)

    processes.clear()

    my_logger.warning('Duration: {}'.format(datetime.datetime.now() - start_time))
    my_logger.warning('All workers finished')

def add_foreign_keys(create = True):
    with open_conn() as connection:
        with connection.cursor() as cursor:
            query = cursor.mogrify(__qry_get_in_table_fks)
            cursor.execute(query)
            rows = cursor.fetchall()
            my_logger.warning("Adding foreign keys if any.")
            for row in rows:
                orig_schema = row[0]
                orig_table = row[2]
                orig_fk_name = row[1]
                orig_columns = ""
                if len(str(row[5]).split(",")) == 1:
                    orig_columns = "\"" + row[5] + "\""
                else:
                    splitted = str(row[5]).split(",")
                    for each in splitted:
                        orig_columns += "," + "\"" + each + "\"" if orig_columns else each
                foreign_schema = row[3]
                foreign_table = row[4]
                foreign_columns = ""
                if len(str(row[6]).split(",")) == 1:
                    foreign_columns = "\"" + row[6] + "\""
                else:
                    splitted = str(row[6]).split(",")
                    for each in splitted:
                        foreign_columns += "," + "\"" + each + "\"" if foreign_columns else each
                qry_create = "ALTER TABLE \"{}\".\"{}\" ADD CONSTRAINT cloned_{} FOREIGN KEY ({}) REFERENCES \"{}\"" \
                             ".\"{}\"({});".format(orig_schema, db_table_cloned, orig_fk_name, orig_columns, foreign_schema,
                                                   foreign_table, foreign_columns)
                if create and not args.no_fks:
                    cursor.execute(qry_create)
                else:
                    my_logger.warning(qry_create)
            my_logger.warning("...Done")

def show_create_table():
    with open_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = '{}' "
                           "AND table_name = '{}' ORDER BY ordinal_position;".format(db_schema, db_table))
            column_names = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()
            my_logger.warning(column_names)
            my_logger.warning("Key column has max value: {}".format(max_value))

def check_cloned_table_existence():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                               "'{}';".format(db_schema, db_table_cloned))
                result = cursor.fetchone()
                if result is not None:
                    my_logger.warning("Table {} was already created.".format(db_table_cloned))
                    return True
                else:
                    cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = '{}' AND table_name = "
                                   "'{}';".format(db_schema, db_table_cloned))
                    result = cursor.fetchone()
                    if result is not None:
                        my_logger.warning("Table {} was already created as master partition.".format(db_table_cloned))
                        return True
                    else:
                        return False
    except Exception as e:
        if "pg_stat_statements" in str(e):
            if "connection" not in locals():
                connection = open_conn()
            with connection.cursor() as cursor:
                my_logger.warning("Creating pg_stat_statements extension")
                cursor.execute("CREATE EXTENSION pg_stat_statements;")
            return check_cloned_table_existence()
        else:
            my_logger.error("Error al chequear existencia de la tabla clonada: {}".format(e))
            sys.exit(501)

def validate_rds_metrics(event, logger, dictio, loop=True):
    payload = Payload()
    cw_cli = cloudwatch.CloudWatchClient(payload, logger)
    cw_cli.check_metrics_before_to_continue(__qry_last_value, 'Processing', event,
                                            dictio, loop)

def linear_regression():
    payload = Payload()
    cw_cli = cloudwatch.CloudWatchClient(payload)
    cw_cli.linear_regression()

def create_new_table_schema():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                if not check_cloned_table_existence():
                    my_logger.warning("Creating table")
                    cursor.execute("CREATE TABLE \"{0}\".\"{1}\" (LIKE \"{0}\".\"{2}\" INCLUDING ALL);".format(
                        db_schema, db_table_cloned, db_table))
                    owner = str(get_table_owner(cursor))
                    my_logger.warning("GRANT ALL ON TABLE \"{0}\".\"{1}\" TO \"{2}\";".format(
                        db_schema, db_table_cloned, owner))
                    cursor.execute("GRANT ALL ON TABLE \"{0}\".\"{1}\" TO \"{2}\";".format(
                        db_schema, db_table_cloned, owner))
                    cursor.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{0}\" TO \"{1}\";".format(db_schema, owner))
                if huge_table(connection):
                    cursor.execute("SELECT table_schema, table_name, column_name, data_type FROM "
                                   "information_schema.columns WHERE "
                                   "table_schema = '{}' "
                                   "AND table_name = "
                                   "'{}' AND data_type = 'integer' ORDER BY ordinal_position;".format(db_schema,
                                                                                                      db_table))
                    result = cursor.fetchall()
                    if result is not None:
                        for each_row in result:
                            my_logger.warning("Changing column " + str(each_row) + " to bigint")
                            cursor.execute("ALTER TABLE \"{0}\".\"{1}\" ALTER COLUMN \"{2}\" TYPE bigint;".format(
                                db_schema,db_table_cloned,str(each_row[2])))
        add_foreign_keys()
    except Exception as e:
        if "already exists" in str(e):
            my_logger.error("Objeto duplicado. Detalle del error: {}".format(e))
        else:
            my_logger.error("Error al crear nueva tabla: {}".format(e))
        sys.exit(500)

def perform_analyze():
    try:
        with open_conn() as conn:
            with conn.cursor() as cursor:
                my_logger.warning("Analyzing table")
                cursor.execute("ANALYZE \"{}\".\"{}\";".format(db_schema, db_table_cloned))
            my_logger.warning("Done")
    except Exception as e:
        my_logger.error(e)

def block_if_autovacuum():
    try:
        with open_conn() as connection:
            my_logger.warning("Checking if there are AccessShareLocks already in table {}".format(db_table))
            with connection.cursor() as cursor:
                _formatted = cursor.mogrify(__qry_check_copies_or_vacuums)
                cursor.execute(_formatted)
                result = cursor.fetchall()
                if result:
                    my_logger.warning("Table {} has queries locking possibility to continue. Please wait for them "
                                      "to finish or kill them. Exiting".format(db_table))
                    my_logger.warning("Listing locking queries details:")
                    for row in result:
                        my_logger.warning(row)
                    sys.exit(404)
        my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al consultar locking queries. Detalle: {}".format(e))
        sys.exit(501)

def compare_count():
    try:
        with open_conn() as conn:
            with conn.cursor() as cursor:
                my_logger.warning("Comparing tables")
                cursor.execute("SELECT count(*) as master, (SELECT count(*) FROM \"{0}\".\"{2}\") as clone "
                               "FROM \"{0}\".\"{1}\";".format(db_schema, db_table, db_table_cloned))
                result = cursor.fetchone()
                if result is not None:
                    my_logger.warning(result)
            my_logger.warning("Done")
    except Exception as e:
        my_logger.error(e)

def drop_cloned_table():
    try:
        with open_conn() as conn:
            with conn.cursor() as cursor:
                my_logger.warning("Truncating table first to avoid fk locks in \"{0}\".\"{1}\";".format(db_schema,
                                                                                             db_table_cloned))
                cursor.execute("TRUNCATE TABLE \"{0}\".\"{1}\";".format(db_schema, db_table_cloned))
                my_logger.warning("Dropping table \"{0}\".\"{1}\";".format(db_schema, db_table_cloned))
                cursor.execute("DROP TABLE \"{0}\".\"{1}\";".format(db_schema, db_table_cloned))
            my_logger.warning("Done")
    except Exception as e:
        my_logger.error("Error in dropping cloned table:" + str(e))
        sys.exit(1)

def list_columns(new = False):
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                # print("Performing vacuum on: {}, time: {}".format(db_table, datetime.datetime.now()))
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' "
                               "AND table_name = '{}' ORDER BY ordinal_position;".format(db_schema, db_table))
                rows = cursor.fetchall()
                column_list = ""
                for row in rows:
                    if not column_list:
                        column_list += 'NEW.\"{}\"'.format(str(row[0])) if new else '\"{}\"'.format(str(row[0]))
                    else:
                        column_list += ", NEW.\"{}\"".format(str(row[0])) if new else ", \"{}\"".format(str(row[0]))
        return column_list
    except Exception as e:
        my_logger.warning("Error al listar columnas de la tabla: {}".format(e))

def get_procedure_qry():
    _qry = """
    CREATE OR REPLACE FUNCTION {0}()
    RETURNS TRIGGER AS $$
    DECLARE
      v_state   TEXT;
      v_msg     TEXT;
      v_detail  TEXT;
      v_hint    TEXT;
      v_context TEXT;
    BEGIN  
      IF TG_OP = 'INSERT' THEN
        INSERT INTO \"{1}\".\"{2}\" ({3}) 
       VALUES ({4});
      ELSIF TG_OP = 'DELETE' THEN
       DELETE FROM \"{1}\".\"{2}\" WHERE \"{5}\" = OLD.\"{5}\";
	  ELSIF TG_OP = 'UPDATE' THEN
       UPDATE \"{1}\".\"{2}\" 
	   SET ({3}) = ({4}) WHERE \"{5}\" = NEW.\"{5}\";
      END IF;
      RETURN NULL;
      EXCEPTION 
      WHEN OTHERS THEN
        get stacked diagnostics
        v_state   = returned_sqlstate,
        v_msg     = message_text,
        v_detail  = pg_exception_detail,
        v_hint    = pg_exception_hint,
        v_context = pg_exception_context;

		RAISE NOTICE E'Got exception:
			state  : %
			message: %
			detail : %
			hint   : %
			context: %', v_state, v_msg, v_detail, v_hint, v_context;

		RAISE NOTICE E'Got exception:
			SQLSTATE: % 
			SQLERRM: %', SQLSTATE, SQLERRM; 
		
        IF TG_OP = 'INSERT' THEN 
            RAISE NOTICE 'No es posible insertar en la tabla clonada';
        ELSIF TG_OP = 'DELETE' THEN
            RAISE NOTICE 'No se pudo efectuar el delete en la tabla clonada';
        ELSIF TG_OP = 'UPDATE' THEN
            RAISE NOTICE 'No se pudo efectuar el update en la tabla clonada';
        ELSE
            RAISE NOTICE 'Unknown error';
        END IF;
      RETURN NULL;
    END;$$
    LANGUAGE plpgsql;
    """
    return _qry.format(__proc_name, db_schema, db_table_cloned, list_columns(), list_columns(True), db_column)

def check_trigger_existence():
    try:
        with open_conn() as connection:
            my_logger.warning("Checking if trigger already exists")
            with connection.cursor() as cursor:
                _formatted = cursor.mogrify(__check_tg)
                cursor.execute(_formatted)
                result_same = cursor.fetchall()
                if result_same:
                    my_logger.warning("Trigger exists. Details:")
                    for row in result_same:
                        my_logger.warning(row)
                _formatted = cursor.mogrify(__check_other_tg)
                cursor.execute(_formatted)
                result_other = cursor.fetchall()
                if result_other:
                    if args.force_continue:
                        my_logger.warning("There are other triggers in original table. Script will continue anyways.")
                    else:
                        my_logger.warning("There are other triggers in original table. Cannot continue")
                    for row in result_other:
                        my_logger.warning(row)
        if result_other and not args.force_continue:
            sys.exit(404)
        my_logger.warning("...Done")
        return result_same
    except Exception as e:
        my_logger.error("Error al chequear la existencia del trigger: {}".format(e))
        sys.exit(501)

def create_trigger():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                if not check_cloned_table_existence():
                    create_new_table_schema()
                owner = str(get_table_owner(cursor))
                cursor.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{0}\" TO \"{1}\";".format(db_schema, owner))
                tg_exists = check_trigger_existence()
                my_logger.warning("Creating Procedure")
                f_query = cursor.mogrify(get_procedure_qry())
                cursor.execute(f_query)
                my_logger.warning("...Done")
                if not tg_exists:
                    my_logger.warning("Creating Trigger")
                    tg_query = cursor.mogrify(__qry_create_trigger)
                    cursor.execute(tg_query)
                    my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al crear trigger y stored procedure: {}".format(e))
        sys.exit(500)

def drop_old_table():
    try:
        with open_conn() as conn:
            with conn.cursor() as cursor:
                my_logger.warning("Truncating table first to avoid fk locks in \"{0}\".\"{1}_old\";".format(db_schema,
                                                                                                        db_table))
                cursor.execute("TRUNCATE TABLE \"{0}\".\"{1}_old\";".format(db_schema, db_table))
                my_logger.warning("Dropping table \"{0}\".\"{1}_old\";".format(db_schema, db_table))
                cursor.execute("DROP TABLE \"{0}\".\"{1}_old\";".format(db_schema, db_table))
            my_logger.warning("Done")
    except Exception as e:
        my_logger.error("Error in dropping old table:" + str(e))
        sys.exit(1)

def drop_pglogical():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                my_logger.warning("Dropping extension pglogical if exists")
                cursor.execute("DROP EXTENSION IF EXISTS pglogical")
                my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al droppear la extension pglogical: {}".format(e))
        sys.exit(500)

def get_table_sequences(old = True):
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                if old:
                    formatted_qry = cursor.mogrify(__qry_sequences_old)
                else:
                    formatted_qry = cursor.mogrify(__qry_sequences)
                cursor.execute(formatted_qry)
                result = cursor.fetchall()
        return result
    except Exception as e:
        pprint("Error getting sequence names:" + str(e))
        sys.exit(1)

def alter_sequence_owner(old = True):
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                rows = get_table_sequences(old)
                for row in rows:
                    seq_name = str(row[0])
                    seq_column_name = str(row[1])
                    qry_alters = "ALTER SEQUENCE \"{0}\".\"{1}\" OWNED BY \"{0}\".\"{2}\".\"{3}\";".format(db_schema,
                                                                                                           seq_name,
                                                                                                           db_table,
                                                                                                           seq_column_name)
                    cursor.execute(qry_alters)
    except Exception as e:
        pprint("Error getting altering sequence column owner:" + str(e))
        sys.exit(1)

def get_table_owner(cursor):
    cursor.execute("SELECT tableowner FROM pg_tables WHERE schemaname = '{}' AND tablename = "
                   "'{}';".format(db_schema, db_table))
    result = cursor.fetchone()
    return result[0]

def perform_table_replacement():
    try:
        drop_pglogical()
        connection = open_uncommited_conn()
        my_logger.warning("Performing next operations in a single transaction")
        with connection.cursor() as cursor:
            my_logger.warning("Altering cloned table owner")
            cursor.execute("ALTER TABLE \"{}\".\"{}\" OWNER TO \"{}\";".format(db_schema, db_table_cloned,
                                                                               str(get_table_owner(cursor))))
            my_logger.warning("Altering original table name with suffix OLD")
            cursor.execute("ALTER TABLE \"{0}\".\"{1}\" RENAME TO \"{1}_old\";".format(db_schema, db_table))
            my_logger.warning("Altering cloned table name to original")
            cursor.execute("ALTER TABLE \"{0}\".\"{1}\" RENAME TO \"{2}\";".format(db_schema, db_table_cloned,
                                                                                       db_table))
        my_logger.warning("Committing transaction")
        connection.commit()
        connection.close()

        my_logger.warning("Altering sequence (if exists) to new schema.table.column OWNER")
        alter_sequence_owner()
        my_logger.warning("...Done")

    except Exception as e:
        my_logger.error("Error al realizar el renaming de tablas: {}".format(e))
        if "connection" in locals():
            connection.rollback()
        sys.exit(500)

def drop_trigger(table_renamed=False):
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                my_logger.warning("Dropping Trigger")
                if table_renamed:
                    cursor.execute(__drop_tg_old)
                else:
                    cursor.execute(__drop_tg)
                my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al droppear el trigger: {}".format(e))
        sys.exit(500)

def drop_procedure():
    try:
        with open_conn() as connection:
            with connection.cursor() as cursor:
                my_logger.warning("Dropping Trigger Procedure")
                cursor.execute(__drop_proc)
                my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al droppear el trigger procedure: {}".format(e))
        sys.exit(500)

def check_blocked_hours():
    try:
        now = datetime.datetime.now()
        hour = int(now.strftime("%H"))
        if hour > 9 and hour < 23 and not args.force_drop:
            raise Exception("Cannot run in productive hours")
        elif args.force_drop:
            my_logger.warning("Forcing execution besides time : {}".format(hour))
        else:
            my_logger.warning("Actual hour is enabled to execute this action: {}".format(hour))
    except Exception as e:
        my_logger.error("Error: {}".format(e))
        sys.exit(500)

def check_referenced_foreign_keys(required=False):
    try:
        with open_conn() as connection:
            my_logger.warning("Checking if there are tables with fks referencing {}".format(db_table))
            with connection.cursor() as cursor:
                _formatted = cursor.mogrify(__qry_check_referencing_tables)
                cursor.execute(_formatted)
                result = cursor.fetchall()
                if result:
                    if not args.force_continue or required:
                        my_logger.warning("Table {} has other tables references with foreign keys. Please DROP them before "
                                      "continue or use --force-continue flag".format(db_table))
                    elif args.force_continue:
                        my_logger.warning("Table {} has other tables references with foreign keys. Continuing "
                                          "anyways".format(db_table))
                    my_logger.warning("Foreign keys list")
                    for row in result:
                        my_logger.warning(row)
        if result and (not args.force_continue or required):
            sys.exit(1)
        my_logger.warning("...Done")
    except Exception as e:
        my_logger.error("Error al chequear tablas que tengan foreign key sobre la operada: {}".format(e))
        sys.exit(501)


def begin_processing():
    create_trigger()
    if "timestamp" in str(identity_column_type) or "date" in str(identity_column_type):
        if not args.force_jobs:
            my_logger.warning(
            "Eligió la columna: {} del tipo {}. Se reducirán los workers a 1 para evitar "
            "duplicidad de datos.".format(db_column, identity_column_type))
        else:
            my_logger.warning(
                "Eligió la columna: {} del tipo {}. Dado que está forzando el uso de múltiples workers, "
                "tenga en cuenta que ante un episodio de crash, no será posible asegurar consistencia al reanudar si "
                "la tabla no tiene ningún campo UNIQUE/PK y se recomienda empezar desde cero."
                ".".format(db_column, identity_column_type))
    populate_cloned_table(begin_with)
    perform_analyze()

def main():
    """
    Actions to run
    """
    global max_value
    max_value = int(get_master_max_value(open_conn()))
    if str(action).lower() == "create-new-table":
        create_new_table_schema()
    elif str(action).lower() == "start":
        check_referenced_foreign_keys()
        begin_processing()
    elif str(action).lower() == "start-complete-process":
        check_referenced_foreign_keys(True)
        begin_processing()
        block_if_autovacuum()
        perform_table_replacement()
        drop_trigger(True)
        drop_procedure()
    elif str(action).lower() == "replace-with-coned-table":
        if not args.force_fivetran_swap_ok:
            my_logger.warning("!!!! Antes de continuar con el RENAME y ROMPER el conector de fivetran, verificar con BIGDATA este proceso !!!!!")
            my_logger.warning("!!!! Si todo esta bien, saltar este warning agregando '--force-fivetran-swap-ok' como parametro !!!!!")
        else:
            check_referenced_foreign_keys(True)
            block_if_autovacuum()
            perform_table_replacement()
            drop_trigger(True)
            drop_procedure()
    elif str(action).lower() == "show-create-table":
        pprint("Show Table Structure")
        show_create_table()
    elif str(action).lower() == "create-trigger":
        block_if_autovacuum()
        create_trigger()
    elif str(action).lower() == "drop-trigger":
        drop_trigger(True)
    elif str(action).lower() == "drop-trigger-non-renamed":
        check_blocked_hours()
        block_if_autovacuum()
        drop_trigger()
    elif str(action).lower() == "drop-procedure":
        drop_procedure()
    elif str(action).lower() == "drop-trigger-and-procedure":
        drop_trigger(True)
        drop_procedure()
    elif str(action).lower() == "drop-trigger-and-procedure-non-renamed":
        check_blocked_hours()
        block_if_autovacuum()
        drop_trigger()
        drop_procedure()
    elif str(action).lower() == "check-instance-metrics":
        validate_rds_metrics(event, my_logger, {'wait_per_transaction':speed, 'NoLimits': args.no_limits,
                                                'type':identity_column_type}, False)
    elif str(action).lower() == "regression":
        linear_regression()
    elif str(action).lower() == "perform-analyze":
        perform_analyze()
    elif str(action).lower() == "drop-old-table":
        check_blocked_hours()
        drop_old_table()
    elif str(action).lower() == "compare-count":
        compare_count()
    elif str(action).lower() == "drop-coned-table":
        check_blocked_hours()
        drop_cloned_table()
    elif str(action).lower() == "list-columns":
        list_columns()
    elif str(action).lower() == "add-fks":
        add_foreign_keys()
    elif str(action).lower() == "show-fks":
        add_foreign_keys(False)
    elif str(action).lower() == "get-table-sequences":
        my_logger.warning(get_table_sequences(False))
    elif str(action).lower() == "get-old-table-sequences":
        my_logger.warning(get_table_sequences(True))
    elif str(action).lower() == "alter-table-sequence-owner":
        alter_sequence_owner()
    elif str(action).lower() == "drop-pglogical":
        drop_pglogical()
    elif str(action).lower() == "check-foreign-keys":
        check_referenced_foreign_keys()
    elif str(action).lower() == "check-blocked-hours":
        check_blocked_hours()
    elif str(action).lower() == "check-if-locking-queries":
        block_if_autovacuum()
    else:
        print("There are no actions with this name")

if __name__ == '__main__':
    if not check_column_has_index():
        sys.exit(404)
    manager = multiprocessing.Manager()
    event = manager.Event()
    queue = manager.Queue()
    dictio = manager.dict()
    main()
