#!/usr/bin/python3

import signal
import sys
import argparse
import logging
import os
import psycopg2
import pprint
import time
import calendar
import threading
import boto3
from botocore.config import Config
from datetime import datetime, timedelta
import re
import json

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
from lib.settings import Settings as X1X_settings

logger = None
result_available_vacuum = threading.Event()
result_available_rds = threading.Event()

check_metrics = 30
pidfile = "/tmp/pg_archiver.pid"
version = "0.2.8"

def paramParser():
    parser = argparse.ArgumentParser(description='Purge rows for Postgres', add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s version {}'.format(version))
    subparsers = parser.add_subparsers(dest="command", help="You must choose an option")

    purge_parse = subparsers.add_parser('purge', help="to start removing data")
    purge_parse.add_argument("-c", "--country", help="country prefix [xx, dev]", type=str, required=True)
    purge_parse.add_argument("-e", "--endpoint", help="endpoint instance to connect" , type=str, required=True)
    purge_parse.add_argument("-P", "--port", help="database server port", type=int, required=True)
    purge_parse.add_argument("-u", "--user", help="user to connect to the database", default="dba_test_service", type=str, required=False)
    purge_parse.add_argument("-p", "--password", help="password to connect to the database", type=str, required=False)
    # purge_parse.add_argument("--ask-password", help="It will ask for the password to avoid send on the command line", type=str, required=False)
    purge_parse.add_argument("-d", "--database", help="database name", type=str, required=True)
    purge_parse.add_argument("-t", "--table", help="table name", type=str, required=True)
    purge_parse.add_argument("--where", help="WHERE clause to limit which rows to archive", type=str, required=True)
    purge_parse.add_argument("--limit", help="Number of rows to fetch and archive per statement. (default: %(default)s)", default="1000", type=int, required=False)
    purge_parse.add_argument("--progress", help="Print progress information every X rows", default="1000", type=int, required=False)
    # purge_parse.add_argument("--check-metrics", help="Check CPU, process, and RDS metrics every X seconds", default="30", type=int, required=False)
    purge_parse.add_argument("-f", "--force-delete-without-pk", help="Force to delete rows without a PK.", default="False", type=bool, required=False)
    # purge_parse.add_argument("--max-load", help="Examine PG_STAT_ACTIVITY after every chunk, and pause if the number of processes is higher than the threshold", default="25", type=int, required=False)
    purge_parse.add_argument("--max-cpu", help="Examine RDS CPUUtilization percent after every chunk, and pause if the number of CPU usage is higher than the threshold. (default: %(default)s)", default="50", type=int, required=False)
    purge_parse.add_argument("--max-disk-queue", help="Examine RDS DiskQueueDepth after every chunk, and pause if the number of I/O queuee is higher than the threshold. (default: %(default)s)", default="35", type=int, required=False)
    purge_parse.add_argument("--min-burst-balance", help="Examine RDS BurstBalance after every chunk, and pause if the number of Burst Balance is lower than the threshold. (default: %(default)s)", default="80", type=int, required=False)
    purge_parse.add_argument("--vacuum-prcent", help="Running VACUUM when the percent of dead tuples is more than the default value. (default: %(default)s)", default="0.1", type=float, required=False)
    purge_parse.add_argument("--check-interval", help="Sleep time between every chunk in seconds. (default: %(default)s)", default="0.3", type=float, required=False)

    if len(sys.agv)==1:
        parser.print_help(sys.stderr)
        sys.exit(100)

    args = parser.parse_args()

    return args

def customLogger(name, logs_backup_dir, endpoint):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(screen_handler)

    file_handler = logging.FileHandler(os.path.join(logs_backup_dir, name + endpoint + ".log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)    

    return logger

def signal_handler(signal, frame):
    global logger

    remove_pidfile()  

    logger.info("Finished")
    sys.exit(4)

def remove_pidfile():
    if os.path.isfile(pidfile):
        os.remove(pidfile)      

class PGArchiver:
    vacuum_tables = set()
    logger = None
    pg_conn = None
    pg_cur = None    
    force_delete_without_pk = False 
    vacuum_percent = 0.1
    max_load = 25
    max_cpu = 25
    max_disk_queue = 25
    min_burst_balance = 80    
    check_metric_time = 60
    pg_version = 0
    sql_pg_version = '''
                SELECT setting FROM pg_settings where name IN ('server_version')
                '''
    sql_slot_repl_more_than_10 = '''
            SELECT
                slot_name,
                slot_type,
                database,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replicationSlotLag,
                active
            FROM
                pg_replication_slots
            WHERE 
                database = '%s'
            ORDER BY
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
                '''
    sql_slot_repl_less_than_10 = '''
            SELECT
                slot_name,
                slot_type,
                database,
                pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn)) AS replicationSlotLag,
                active,
                plugin
            FROM
                pg_replication_slots
            WHERE 
                database = '%s'                
                '''
    sql_column_names = '''
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '%s'
                    '''
    sqlVacuumPercent = '''
                    SELECT ROUND(CAST(FLOAT8 ((n_dead_tup * 100) / (CASE WHEN n_live_tup = 0 THEN 1 ELSE n_live_tup END) ) AS NUMERIC),2) FROM pg_stat_user_tables where relname = '%s'    
                    '''
    sql_column_check_indexes = '''
                    SELECT
                    table_name,
                    index_name,
                    string_agg(column_name, ',')
                    FROM (
                        SELECT
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
                            AND t.relkind = 'r'
                            AND t.relname LIKE '%s'
                        ORDER BY table_name, index_name, i
                        ) raw
                    GROUP BY table_name, index_name
                        '''
    sql_primary_key = '''
                    SELECT c.column_name, c.data_type, c.table_schema
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                    WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '%s';
                    '''
    sql_foreign_key = '''
            SELECT
                tc.table_schema, 
                tc.constraint_name, 
                tc.table_name, 
                kcu.column_name, 
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';    
            '''

    def __init__(self, logger, args):
        self.logger = logger
        self.pg_instance_endpoint = args.endpoint
        self.pg_instance_port = args.port
        self.pg_database = args.database
        self.pg_table = args.table
        self.pg_where = args.where
        self.pg_limit = args.limit
        self.pg_user = args.user
        self.pg_password = args.password
        self.force_delete_without_pk  = args.force_delete_without_pk
        self.vacuum_percent = args.vacuum_percent
        # self.check_metric_time = args.check_metrics
        
    def Connection(self):
        try:
            self.logger.info('Connecting to the PostgreSQL database %s:%d' % (self.pg_instance_endpoint, int(self.pg_instance_port)))
            self.pg_conn = psycopg2.connect(dbname=self.pg_database, user=self.pg_user, port=self.pg_instance_port, host=self.pg_instance_endpoint, password=self.pg_password, connect_timeout=60)
            self.pg_conn.autocommit = False
            self.pg_cur = self.pg_conn.cursor()
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            return False

    def Close(self):
        if self.pg_conn is not None:
            self.pg_conn.close()
            return True
        else:
            return False

    def checkDeadTuples(self):
        current_percent = float(self.__getVacuumPercent(self.pg_table))

        while True:
            try:
                if current_percent > self.vacuum_percent:
                    result_available_vacuum.clear()
                    logger.info("%-30s%-30s%-30s%-30s%-30s%-30s" % ("-", "-", "-", "-", "-", "Vacuum ["+str(current_percent)+"]"))
                    self.__runVacuum(self.pg_table)
                    result_available_vacuum.set()
                else:
                    result_available_vacuum.set()
            except Exception as e:
                sys.exit(5)                        
            
            time.sleep(self.check_metric_time)

    def __getVacuumPercent(self, table_name):
        try:
            self.pg_cur.execute(self.sqlVacuumPercent % (table_name))
            if (row:= self.pg_cur.fetchone()) is not None:
                return row[0]
            else:
                return 0
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)    

    def __runVacuum(self, table_name):
        try:
            old_isolation_level = self.pg_conn.isolation_level
            self.pg_conn.set_isolation_level(0)
            self.pg_cur.execute('vacuum %s' % (table_name))
            self.pg_conn.set_isolation_level(old_isolation_level)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(2)                                 

    def GetInstanceName(self):
        return self.pg_instance_endpoint.split(".")[0]

    def GetColumnsFromTable(self, table_name):
        try:
            columns_struct = []

            self.pg_cur.execute(self.sql_column_names % (table_name))
            rows = self.pg_cur.fetchall()

            if rows:
                for row in rows:
                    columns_struct.append(row[0])

            return columns_struct
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)  

    def GetColumnsFromWhere(self, table_name, stmt_where):
        try:
            columns_struct = []
            columns_where = []

            self.pg_cur.execute(self.sql_column_names % (table_name))
            rows = self.pg_cur.fetchall()

            if rows:
                for row in rows:
                    columns_struct.append(row[0])

                where_data = stmt_where.split(" ")
                for i in where_data:
                    if i in columns_struct:
                        columns_where.append(i)

            return columns_where
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)      

    def GetPKColumn(self, table_name):    
        try:
            self.pg_cur.execute(self.sql_primary_key % (table_name))
            rows = self.pg_cur.fetchall()

            if rows and len(rows) == 1:
                return 'pk', rows[0][0]
            elif rows and len(rows) > 1:
                return 'composite_pk', ''
            else:
                return 'no_pk', ''                

        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)  

    def CheckColumnIndex(self, table_name, column_name):
        try:
            self.pg_cur.execute(self.sql_column_check_indexes % (table_name))
            rows = self.pg_cur.fetchall()

            if rows:
                for row in rows:
                    if row[2].split(',')[0] == column_name:
                        return True
                return False
            else:
                self.logger.info("There is no indexes created using the column %s" % (column_name))
                return False

        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)    

    def GetPGVersion(self):
        try:
            self.pg_cur.execute(self.sql_pg_version)
            if (row:= self.pg_cur.fetchone()) is not None:
                self.pg_version = row[0]
            else:
                self.logger.error("Cant not find the Postgres version. Abort")
                sys.exit(6)    
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(6)         

    def CheckSlotReplicationSize(self):
        pg_version_array = self.pg_version.split(".")
        if len(pg_version_array) > 2:
            self.pg_version = pg_version_array[0]+"."+pg_version_array[1]

        if float(self.pg_version) >= 10:
            try:
                self.pg_cur.execute(self.sql_slot_repl_more_than_10 % (self.pg_database))
                if (row:= self.pg_cur.fetchone()) is not None:
                    self.logger.warning("There is a replication slot configured for this database '%s' - Slot name '%s' - Replication slot lag '%s'"% (self.pg_database, row[0], row[3]))
                else:
                    self.logger.info("There is NO replication slot configured for this database '%s'" % (self.pg_database))
            except (Exception, psycopg2.DatabaseError) as error:
                self.logger.error(error)
                sys.exit(6)             
        else:
            try:
                self.pg_cur.execute(self.sql_slot_repl_less_than_10 % (self.pg_database))
                if (row:= self.pg_cur.fetchone()) is not None:
                    self.logger.warning("There is a replication slot configured for this database '%s' - Slot name '%s' - Replication slot lag '%s'"% (self.pg_database, row[0], row[3]))
                else:
                    self.logger.info("There is no replication slot configured for this database '%s'" % (self.pg_database))
            except (Exception, psycopg2.DatabaseError) as error:
                self.logger.error(error)
                sys.exit(6)                 

    def Commit(self):
        try:
            self.pg_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("commit error - %s" % error)

    def Rollback(self):
        self.pg_conn.rollback()

    def PrepareDeleteStmt(self):
        status, column = self.GetPKColumn(self.pg_table)
        if status == 'pk':
            return "DELETE FROM \"{0}\" WHERE {1} = any (array(SELECT {1} FROM \"{0}\" WHERE {2} ORDER BY {1} LIMIT {3}))".format(self.pg_table, column, self.pg_where, self.pg_limit)
        elif status in ['no_pk', 'composite_pk']:
            if self.force_delete_without_pk:
                columns = self.GetColumnsFromTable(self.pg_table)
                columns_string = ",".join(columns)
                return "DELETE FROM \"{0}\" WHERE ({1}) IN (SELECT {1} FROM \"{0}\" WHERE {2} LIMIT {3})".format(self.pg_table, columns_string, self.pg_where, self.pg_limit)
            else:
                if status == 'no_pk':
                    self.logger.error("Abort. There is no a PK for %s table. It's not supported. If you want continue use `--force-delete-without-pk`" % (self.pg_table))
                    sys.exit(6)  
                elif status == 'composite_pk':
                    self.logger.error("Abort. There is a composite PK for %s table. It's not supported. If you want continue use `--force-delete-without-pk`" % (self.pg_table))
                    sys.exit(6)                      

    def Purge(self, delete):
        try:
            self.pg_cur.execute(delete)
            self.pg_conn.commit()

            return self.pg_cur.rowcount
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("commit error - %s" % error)

            remove_pidfile()  

            sys.exit(101)

class ThreadMetrics(threading.Thread):
    # overriding constructor
    def __init__(self, instance_name, region, aws_profile, check_metric_time, max_cpu, max_disk_queue, min_burst_balance):
        # calling parent class constructor
        threading.Thread.__init__(self, daemon=True)
        self.instance_name = instance_name
        self.region = region
        self.aws_profile = aws_profile
        self.check_metric_time = check_metric_time
        self.max_cpu = max_cpu
        self.max_disk_queue = max_disk_queue
        self.min_burst_balance = min_burst_balance

    # define your own run method
    def run(self):
        queries = { 'metrics': [{
                'Id': 'request1',
                'Label': "CPUUtilization",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': self.instance_name
                            }
                        ]
                    },
                    'Period': 60,
                    'Stat': 'Average'
                }
            },
            {
                'Id': 'request2',
                'Label': "DiskQueueDepth",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'DiskQueueDepth',
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': self.instance_name
                            }
                        ]
                    },
                    'Period': 60,
                    'Stat': 'Average'
                }
            },
            {
                'Id': 'request3',
                'Label': "BurstBalance",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'BurstBalance',
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': self.instance_name
                            }
                        ]
                    },
                    'Period': 60,
                    'Stat': 'Average'
                }
            }]
            }

        api_config = Config(
            region_name = self.region,
            connect_timeout = 5,
            read_timeout = 60,
            retries = {
                'max_attempts': 50,
                'mode': 'standard'
            }
        )
        session = boto3.session.Session(profile_name=self.aws_profile)
        cloudwatch = session.client('cloudwatch', config=api_config)

        while True:
            try:
                # get metrics from the last 5 minutes
                response = cloudwatch.get_metric_data(
                    MetricDataQueries = queries['metrics'],
                    StartTime = (datetime.now() - timedelta(seconds=60 * 5)).timestamp(),
                    EndTime = datetime.now().timestamp(),
                )

                if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
                    if len(response['MetricDataResults'][0]['Values']) > 0:
                        CPUUtilization = round(response['MetricDataResults'][0]['Values'][0], 2)
                    else:
                        CPUUtilization = -1

                    if len(response['MetricDataResults'][1]['Values']) > 0:
                        DiskQueueDepth = round(response['MetricDataResults'][1]['Values'][0], 2)
                    else:
                        DiskQueueDepth = -1

                    if len(response['MetricDataResults'][2]['Values']) > 0:
                        BurstBalance = round(response['MetricDataResults'][2]['Values'][0], 2)
                    else:
                        BurstBalance = 101

                    if CPUUtilization < self.max_cpu and DiskQueueDepth < self.max_disk_queue and BurstBalance >= self.min_burst_balance:
                        result_available_rds.set()
                        logger.info("%-30s%-30s%-30.2f%-30.2f%-30.2f%-30s" % ("-", "-", CPUUtilization, DiskQueueDepth, BurstBalance, "-"))
                        time.sleep(self.check_metric_time)
                    else:
                        result_available_rds.clear()
                        logger.info("%-30s%-30s%-30.2f%-30.2f%-30.2f%-30s" % ("waiting", "-", CPUUtilization, DiskQueueDepth, BurstBalance, "-"))
                        time.sleep(30)
            except Exception as e:
                sys.exit(5)    

def PrintListHeader(max_cpu, max_disk_queue, min_burst_balance, vacuum_percent):
    logger.info("%-30s%-30s%-30s%-30s%-30s%-30s" % ("Total rows deleted", "Time spent in sec", "CPUUtilization ["+str(max_cpu)+"]", "DiskQueueDepth ["+str(max_disk_queue)+"]", "BurstBalance ["+str(min_burst_balance)+"]", "Vacuum ["+str(vacuum_percent)+"]"))
    logger.info("-------------------------------------------------------------------------------------------------------------------------------------------------------------------")

def check_secret():
    try:
        i_dynamo = dynamo.DynamoClient('dev')
        var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
        return var['value']
    except Exception as e:
        i_dynamo = dynamo.DynamoClient('xx')
        var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
        return var['value']

def main():
    global logger, pidfile

    args = paramParser()

    aws_profile = X1X_settings.AWS_PROFILE[args.country]
    region = X1X_settings.REGION[args.country]    

    logger = customLogger( os.path.basename(__file__), '/var/log/X1X' , "_"+args.endpoint+"_"+args.database+"."+args.table)

    if args.user == 'dba_test_service' and not args.password:
        args.password = check_secret()

    if "test-connection" in args.command:
        pass
    elif "dry-run" in args.command:
        pass
    elif "purge" in args.command:
        pgarchiver = PGArchiver(
                    logger,
                    args
                    )        

        pid = str(os.getpid())

        # pidfile = python file name + MS_instance_name + database + table
        pidfile = os.path.join("/tmp", os.path.basename(__file__)+"_"+pgarchiver.GetInstanceName()+"_"+args.database+"_"+args.table+".pid")

        if os.path.isfile(pidfile):
            print ("There this another process running against same DB and table. pidfile: %s - Exiting" % pidfile)
            sys.exit()
        else:
            f = open(pidfile, 'w')
            f.write(pid)
            f.close()

        if pgarchiver.Connection():
            logger.info("Connected")

            pgarchiver.GetPGVersion()
            pgarchiver.CheckSlotReplicationSize()

            metrics = ThreadMetrics(pgarchiver.GetInstanceName(), region, aws_profile, check_metrics, args.max_cpu, args.max_disk_queue, args.min_burst_balance)
            metrics.start()

            columns = pgarchiver.GetColumnsFromWhere(args.table, args.where)

            for c in columns:
                if pgarchiver.CheckColumnIndex(args.table, c):
                    logger.info("Column %s have a correct index" % (c))

                    delete = pgarchiver.PrepareDeleteStmt()
                    logger.info("Command to run for each transaction - statement: %s" % (delete))

                    logger.info("RDS monitoring metrics every %d seconds" % (check_metrics))
                    logger.info("VACUUM monitoring every %d seconds against '%s' table" % (check_metrics, args.table))

                    dead_tuples = threading.Thread(target=pgarchiver.checkDeadTuples, daemon=True)
                    dead_tuples.start()

                    sum_progress = 0
                    sum_total_progress = 0

                    # logger.info("Waiting for metrics before to start the purge process")

                    PrintListHeader(args.max_cpu, args.max_disk_queue, args.min_burst_balance, args.vacuum_percent)

                    ts_start = calendar.timegm(time.gmtime())
                    while True:
                        result_available_vacuum.wait()
                        result_available_rds.wait()

                        rows_deleted = pgarchiver.Purge(delete)

                        if rows_deleted < 0:
                            logger.info("There was an error retrieving the remaining rows pending to delete")
                            logger.info("Statement - %s" % (delete))
                            # sys.exit(100)
                        elif rows_deleted > 0:
                            sum_progress += int(args.limit)
                            sum_total_progress += rows_deleted

                            if sum_progress >= args.progress:
                                ts_end = calendar.timegm(time.gmtime())
                                sum_progress = 0
                                logger.info("%-30i%-30i%-30s%-30s%-30s%-30s" % (sum_total_progress, (ts_end-ts_start), "-", "-", "-", "-"))
                                ts_start = calendar.timegm(time.gmtime())
                        elif rows_deleted == 0:
                            logger.info("There is no more rows to remove. Job finished ok")
                            break
                        else:
                            logger.info("nothing to do")
                            break

                        time.sleep(args.check_interval)
                else:
                    logger.error("There is no index for column %s.%s, please check the WHERE statement and rerun the script again" % (args.table, c))
                    logger.error("The columns into `--where` param needs to have an index to avoid full scan in the table")

            if pgarchiver.Close():
                logger.info("Connection closed")
            else:
                logger.error('Connection lost, nothing to close')        
        else:
            logger.error("Can not connect to Postgres")

        remove_pidfile()
    else:
        pass

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()