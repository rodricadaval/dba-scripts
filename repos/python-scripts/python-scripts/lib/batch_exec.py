from datetime import datetime, timedelta
import boto3
from botocore.config import Config
from itertools import dropwhile
from multiprocessing import Process, Manager
import multiprocessing
import os.path
import psycopg2
import re
from lib.settings import Settings as X1X_settings
import sys
import time
import pprint
import psycopg2.extras
import threading

result_available_rds = multiprocessing.Event()

class ThreadMetrics(threading.Thread):
    # overriding constructor
    def __init__(self, instance_name, region, aws_profile, check_metric_time, internal_dict, logger, args):
        # calling parent class constructor
        threading.Thread.__init__(self, daemon=True)
        self.instance_name = instance_name
        self.region = region
        self.aws_profile = aws_profile
        self.check_metric_time = check_metric_time
        self.max_load = args.max_load
        self.max_cpu = args.max_cpu
        self.max_disk_queue = args.max_disk_queue
        self.min_burst_balance = args.min_burst_balance
        self.internal_dict = internal_dict
        self.logger = logger
        self.parse_args = args

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

        batch_exec = BatchExec(self.logger, self.parse_args)
        batch_exec.Connection()           

        run_vaccum = False
        while (self.internal_dict['force_exit'] == 0):
            try:
                number_processes = batch_exec.GetNumberProcessesRunning()

                if not run_vaccum:
                    run_vaccum = batch_exec.checkDeadTuples()

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

                    if number_processes < self.max_load and CPUUtilization < self.max_cpu and DiskQueueDepth < self.max_disk_queue and BurstBalance >= self.min_burst_balance and run_vaccum == False:
                        result_available_rds.set()
                        self.logger.info("Status - PG Load[%d]: %.2f - CPUUtilization[%d]: %.2f - DiskQueueDepth[%d]: %.2f - BurstBalance[%d]: %.2f" % (self.max_load, number_processes, self.max_cpu, CPUUtilization, self.max_disk_queue, DiskQueueDepth, self.min_burst_balance, BurstBalance))
                        time.sleep(self.check_metric_time)
                    else:
                        result_available_rds.clear()

                        if run_vaccum:
                            self.logger.info("Running Vacuum against '%s' table" % self.parse_args.table)
                            batch_exec.RunVacuum()
                            run_vaccum = False

                        print("Paused all processes - PG Load[%d]: %.2f - CPUUtilization[%d]: %.2f - DiskQueueDepth[%d]: %.2f- BurstBalance[%d]: %.2f" % (self.max_load, number_processes, self.max_cpu, CPUUtilization, self.max_disk_queue, DiskQueueDepth, self.min_burst_balance, BurstBalance))
                        time.sleep(10)
            except Exception as e:
                sys.exit(5)   

        batch_exec.Close()
        # due internal_dict['force_exit'] == 1
        result_available_rds.set()

class ProcessManager(object):
    """ Create processes and wait for them to finish. 
        Throttling is done by adding new processes and removing old ones on the fly. 

    """
    def __init__(self, func, queue, parse_args, result_available_rds, logger, start_processes = 3, max_processes = 10):
        self.start_processes = start_processes
        self.max_processes = max_processes
        self.num_workers = 0
        self.pool = []
        self.func = func
        self.queue = queue
        self.parse_args = parse_args
        self.result_available_rds = result_available_rds
        self.logger = logger

        # workers
        for i in range(self.start_processes):
            self.AddProcesses()     

    def AddProcesses(self):
        if self.num_workers <= self.max_processes:
            # self.logger.info("creating process %d" % (self.num_workers))
            p = Process(target=self.func, args=(self.queue, self.num_workers, self.parse_args, self.result_available_rds, self.logger))
            p.daemon = True
            self.pool.append(p)
            p.start()

            self.num_workers += 1   
        else:
            self.logger.error("Error: cant create more processes, you reached the max_processes = %d" % self.max_processes)  

    def RemoveProcess(self):
        if self.num_workers > 1:
            # print("removing process %d" % (self.num_workers-1))
            p = self.pool.pop()
            p.terminate()
            self.num_workers -= 1   
        else:
            self.logger.error("Error: cant remove more processes, you reached the min_processes = %d" % self.num_workers)              

    def Start(self):
        for p in self.pool:
            if p.is_alive():
                p.start()

    def Wait(self):
        while True:
            if not self.queue.empty():
                time.sleep(1)
                self.logger.info("Waiting for all processes in the queue to finish ...")
            else:
                break

class BatchExec(object):
    pg_conn = None
    pg_cur = None
    __sqlGetProcessesRunning = "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"
    __sqlVacuumPercent = "SELECT ROUND(CAST(FLOAT8 ((n_dead_tup * 100) / (CASE WHEN n_live_tup = 0 THEN 1 ELSE n_live_tup END) ) AS NUMERIC),2) FROM pg_stat_user_tables where relname = '%s'"
    __sqlGetAmountOfRows = "SELECT COUNT(*) FROM %s"
    sql_column_names = '''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '%s'
                '''
    sql_primary_key = '''
                    SELECT c.column_name, c.data_type, c.table_schema
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                    WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '%s';
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
    __pause_file = "/tmp/pause.file"
    __aws_profile = ""
    __region = ""
    __vacuum_tables = set()    

    def __init__(self, logger, args):
        self.instance_name = args.endpoint.split(".")[0]
        self.pg_instance_endpoint = args.endpoint
        self.pg_instance_port = args.port
        self.pg_database = args.database
        self.pg_user = args.user
        self.pg_password = args.password
        self.logger = logger

        if "test-connection" in args.command:
            pass
            # self.vacuum_percent = self.vacuum_percent
            # self.check_interval = self.check_interval
            # self.max_load = self.max_load
            # self.max_cpu = self.max_cpu
            # self.max_disk_queue = self.max_disk_queue
        elif "execute" in args.command:
            self.vacuum_percent = args.vacuum_percent
            self.check_interval = args.check_interval
            self.max_load = args.max_load
            self.max_cpu = args.max_cpu
            self.max_disk_queue = args.max_disk_queue
            self.table = args.table

        self.__aws_profile = X1X_settings.AWS_PROFILE[args.country]
        self.__region = X1X_settings.REGION[args.country]

    def Connection(self):
        try:
            self.logger.info('Connecting to PostgreSQL database %s:%d' % (self.pg_instance_endpoint, int(self.pg_instance_port)))
            self.pg_conn = psycopg2.connect(dbname=self.pg_database, user=self.pg_user, port=self.pg_instance_port, host=self.pg_instance_endpoint, password=self.pg_password, connect_timeout=60)
            self.pg_conn.autocommit = True
            self.pg_cur = self.pg_conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Connection error - %s " % error)
            sys.exit(1)

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
                    # find if the column name exists in the table struct, to exclude any reserved words

                    # find is there is a '=' char, if yes is needed to split
                    if (i.find('=') != -1):
                        i = i.split('=')[0]

                    if i.upper() in columns_struct or i.lower() in columns_struct:
                        columns_where.append(i)

            return columns_where
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

    def CheckIndexes(self, query):
        # split WHERE statements into list
        where = re.split("WHERE", query, flags=re.IGNORECASE)

        if len(where) == 2:
            columns = self.GetColumnsFromWhere(self.table, where[1])   
            have_indexes = False
            for c in columns:
                if self.CheckColumnIndex(self.table, c.lower()):      
                    self.logger.info("Column '%s' have a correct index" % (c.lower()))
                    have_indexes = True  

            if not have_indexes:
                status, column = self.GetPKColumn(self.table)
                if status == 'pk':
                    if column in columns:
                        self.logger.info("Column '%s' have a correct PK" % (column))
                        have_indexes = True  

            return have_indexes
        else:
            self.logger.error("There is an error in the query, seems to be it doesnt have 'WHERE' statement")
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

    def Close(self):
        if self.pg_conn is not None:
            self.pg_conn.close()
            self.logger.info('Database connection closed')
        else:
            self.logger.info('Connection lost, nothing to close')            

    def Commit(self):
        try:
            self.pg_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("commit error - %s" % error)

    def Rollback(self):
        self.pg_conn.rollback()

    def GetNumberProcessesRunning(self):
        try:
            self.pg_cur.execute(self.__sqlGetProcessesRunning)
            row = self.pg_cur.fetchone()[0]
            # self.pg_conn.commit()
            return row
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("GetNumberProcessesRunning error - %s" % error)
            return 0

    def Execute(self, query):
        try:
            # self.__tables_in_query(query)
            self.pg_cur.execute(query)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("execute error - query: %s - error: %s" % (query, str(error).rstrip()))

    def checkDeadTuples(self):
        current_percent = float(self.__getVacuumPercent(self.table))

        try:
            # self.logger.info("Running Vacuum [%.2f]: %.2f" % (self.ags.vacuum_percent, current_percent))
            # self.__runVacuum(self.pg_table)
            if current_percent > self.vacuum_percent:
                return True
            else:
                return False
        except Exception as e:
            sys.exit(5)                        

    def __getVacuumPercent(self, table_name):
        try:
            self.pg_cur.execute(self.__sqlVacuumPercent % (table_name))
            if (row:= self.pg_cur.fetchone()) is not None:
                return row[0]
            else:
                return 0
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("__getVacuumPercent error - %s" % error)

    def RunVacuum(self):
        try:
            old_isolation_level = self.pg_conn.isolation_level
            self.pg_conn.set_isolation_level(0)
            self.pg_cur.execute('vacuum analyze %s' % (self.table))
            self.pg_conn.set_isolation_level(old_isolation_level)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("RunVacuum error - %s" % error)

    def __tables_in_query(self, query):
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", query)

        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

        q = " ".join([re.split("--|#", line)[0] for line in lines])

        tokens = re.split(r"[\s)(;]+", q)

        get_next = False
        for tok in tokens:
            if get_next:
                if tok.lower() not in [""]:
                    self.__vacuum_tables.add(tok)
                get_next = False

            if query.upper().startswith('INSERT INTO'):
                get_next = tok.lower() in ["into"]
            else:
                get_next = tok.lower() in ["from", "join", "into", "update"]                