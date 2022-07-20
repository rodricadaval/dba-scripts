#!/usr/bin/python3

import argparse
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
import getpass
from itertools import dropwhile
import json
import logging
# from lib.rds import RDSClient
from lib.dynamo_rds import DynamoRDSClient
from lib.rds import RDSClient
from lib.batch_exec import BatchExec, ProcessManager, ThreadMetrics, result_available_rds
from lib.settings import Settings as X1X_settings
from lib import dynamo
from multiprocessing import Process, Manager, Queue
import threading
import os.path
import psycopg2
import re
import signal
import sys
import time
import multiprocessing
import threading

version = "2.0.10"

# global variables (NO EDIT)
logger = None
force_exit = False
queue = multiprocessing.Queue(30000)
check_metrics = 30
internal_dict = (Manager()).dict()

def customLogger2(name, logs_backup_dir, endpoint):
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

def customLogger(name, logs_backup_dir, endpoint):
    import multiprocessing, logging
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(\
        '[%(asctime)s| %(levelname)s| %(processName)s] %(message)s')
    handler = logging.FileHandler(os.path.join(logs_backup_dir, name + endpoint + ".log"))
    handler.setFormatter(formatter)

    # this bit will make sure you won't have 
    # duplicated messages in the output
    if not len(logger.handlers): 
        logger.addHandler(handler)
    return logger

def signal_handler(signal, frame):
    global force_exit, internal_dict

    force_exit = True
    internal_dict['force_exit'] = 1

def checkFIleExists(file_path):
    if not os.path.isfile(file_path):
        return False
    else:
        return True

def DoWork(queue, proc_num, parse_args, result_available_rds, logger):
    batch_exec = BatchExec(logger, parse_args)
    batch_exec.Connection()  

    progress = 0
    start_time = datetime.now()

    while True:
        result_available_rds.wait()

        try: 
            line = queue.get()

            batch_exec.Execute(line)
            batch_exec.Commit()

            if progress == parse_args.progress:
                end_time = datetime.now()
                seconds = (end_time - start_time).total_seconds()
                
                logger.info("Proceseed %d lines to Postgres in %d seconds" % (parse_args.progress, seconds))
                
                progress = 0
                start_time = datetime.now()
            else:
                progress += 1
        except Exception as error:
            batch_exec.Close()
            print("close2")
            break

    print("close1")

def GetInstanceName(endpoint):
    return endpoint.split(".")[0]

def iterate_from_line(f, start_from_line):
    return ((l,i) for i, l in dropwhile(lambda x: x[0] < start_from_line, enumerate(f)))

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
    global queue, force_exit, internal_dict

    args = paramParser()

    logger = customLogger2( os.path.basename(__file__), '/var/log/X1X' , "_"+args.endpoint)

    aws_profile = X1X_settings.AWS_PROFILE[args.country]
    aws_region = X1X_settings.REGION[args.country]    

    if args.user == 'dba_test_service' and not args.password:
        args.password = check_secret()

    if "test-connection" in args.command:
        try:
            logger.info("---- Testing connection ----")

            batch_exec = BatchExec(logger, args)
            batch_exec.Connection()   
            logger.info("Connected successfully")
            batch_exec.Close()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            batch_exec.Close()
    elif "execute" in args.command:
        if not checkFIleExists(args.filename):
            print("File doesn't exists")
            sys.exit(7)               

        if args.min_burst_balance <= 0 or args.min_burst_balance > 100:
            logger.error("Param '--min-burst-balance' should be an integer between 1 and 100")
            sys.exit(100)  

        if args.max_load <= args.number_process:
            logger.error("Param '--max-load' should be more than the param '--number-process'")
            sys.exit(101)              

        logger.info("RDS monitoring metrics every %d seconds" % (check_metrics))
        logger.info("VACUUM monitoring every %d seconds" % (check_metrics))      
        logger.info("Starting %d processes" % (args.number_process))  
        logger.info("Starting reading file from position %d" % args.offset)    

        internal_dict['force_exit'] = 0
        metrics = ThreadMetrics(GetInstanceName(args.endpoint), aws_region, aws_profile, check_metrics, internal_dict, logger, args)
        metrics.start()     

        task_manager = ProcessManager(DoWork, queue, args, result_available_rds, logger, args.number_process)       

        file_position = 0
        progress = 0
        start_time = datetime.now()
        have_indexes = False
        for line in iterate_from_line(open(args.filename, "r"), (args.offset-1)):
            result_available_rds.wait()

            if force_exit:
                print("========> Last position executed: %d <========" % file_position)
                break     
            
            query = line[0].rstrip().replace(u'\ufeff', u'')

            # check only the first time, if the columns into WHERE statement have correct INDEX, if not, exit and show the error message
            if not have_indexes:
                if not have_indexes and query.startswith(("insert", "INSERT")):
                    have_indexes = True
                else:
                    batch_exec = BatchExec(logger, args)
                    batch_exec.Connection()   
                    if batch_exec.CheckIndexes(query):
                        batch_exec.Close()
                        have_indexes = True
                    else:
                        batch_exec.Close()
                        logger.error("There are no indexes in the columns into the 'WHERE' statement. Please check the table structure and create a correct one")
                        sys.exit(102)
                
            queue.put(query) 

            if progress == args.progress:
                end_time = datetime.now()
                seconds = (end_time - start_time).total_seconds()
                
                logger.info("Added %d lines to the queue. Current file position %d " % (args.progress, file_position))
                
                progress = 0
                start_time = datetime.now()
            else:
                progress += 1

            file_position = int(line[1]+1)

        task_manager.Wait()   

        internal_dict['force_exit'] = 1
        time.sleep(check_metrics)
        logger.info("Finished - Last position executed %d" % file_position)

def paramParser():
    parser = argparse.ArgumentParser(description='Read lines in batches without really splitting', add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s version {}'.format(version))

    subparsers = parser.add_subparsers(dest="command", help="You must choose an option")

    # test-connection
    test_connection = subparsers.add_parser('test-connection', help="To check connectivity, current table stats and current RDS metrics")
    # test_connection.add_argument("-t", "--type", help="database type, pg or mysql", type=str, required=True)
    test_connection.add_argument("-c", "--country", help="country prefix [uy, co, ar, cl, br, xx1, xx1, tool, dev]", type=str, required=True)
    # test_connection.add_argument("-n", "--name", help="rds instance name to connect" , type=str, required=True)
    test_connection.add_argument("-e", "--endpoint", help="endpoint instance to connect" , type=str, required=True)
    test_connection.add_argument("-u", "--user", help="user to connect to the database. (default: %(default)s)", default="dba_test_service", type=str, required=False)
    test_connection.add_argument("-p", "--password", help="password to connect to the database", type=str, required=False)
    test_connection.add_argument("-P", "--port", help="database server port", type=int, required=True)
    # test_connection.add_argument("--ask-password", help="It will ask for the password to avoid send on the command line", type=str, required=False)
    test_connection.add_argument("-d", "--database", help="database name. (default: %(default)s)", default="postgres", type=str, required=False)

    # execute/proceed SQL file
    execute_parse = subparsers.add_parser('execute', help="To proceed the SQL file against Postgres instance")
    # execute_parse.add_argument("-t", "--type", help="database type, pg or mysql", type=str, required=True)
    execute_parse.add_argument("-c", "--country", help="country prefix [uy, co, ar, cl, br, xx1, xx1, tool, dev]", type=str, required=True)
    # execute_parse.add_argument("-n", "--name", help="rds instance name to connect" , type=str, required=True)
    execute_parse.add_argument("-e", "--endpoint", help="endpoint instance to connect" , type=str, required=True)
    execute_parse.add_argument("-t", "--table", help="table name where you are running all the SQL statements", type=str, required=True)
    execute_parse.add_argument("-f", "--filename", help="file name to import" , type=str, required=True)
    execute_parse.add_argument("--progress", help="print progress information every X rows. (default: %(default)s)" , default="10000", type=int, required=False)
    execute_parse.add_argument("-o", "--offset", help="begin at the Nth line. (default: %(default)s)" , default="1", type=int, required=False)
    execute_parse.add_argument("-u", "--user", help="user to connect to the database. (default: %(default)s)", default="dba_test_service", type=str, required=False)
    execute_parse.add_argument("-p", "--password", help="password to connect to the database", type=str, required=False)
    execute_parse.add_argument("-P", "--port", help="database server port", type=int, required=True)
    # execute_parse.add_argument("--ask-password", help="It will ask for the password to avoid send on the command line", type=int, required=False)
    execute_parse.add_argument("-d", "--database", help="database name. (default: %(default)s)", default="postgres", type=str, required=False)
    execute_parse.add_argument("--check-interval", help="Sleep time between every chunk in seconds. (default: %(default)s)", default="0.1", type=float, required=False)
    execute_parse.add_argument("--number-process", help="Number of processes running in parallel reading the file. (default: %(default)s)", default="5", type=int, required=False)
    execute_parse.add_argument("--max-load", help="Examine PG_STAT_ACTIVITY after every chunk, and pause if the number of processes is higher than the threshold. (default: %(default)s)", default="25", type=int, required=False)
    execute_parse.add_argument("--max-cpu", help="Examine RDS CPUUtilization percent after every chunk, and pause if the number of CPU usage is higher than the threshold. (default: %(default)s)", default="50", type=int, required=False)
    execute_parse.add_argument("--max-disk-queue", help="Examine RDS DiskQueueDepth after every chunk, and pause if the number of I/O queuee is higher than the threshold. (default: %(default)s)", default="35", type=int, required=False)
    execute_parse.add_argument("--min-burst-balance", help="Examine RDS BurstBalance after every chunk, and pause if the number of Burst Balance is lower than the threshold. (default: %(default)s)", default="80", type=int, required=False)
    execute_parse.add_argument("--vacuum-prcent", help="Running VACUUM when the percent of dead tuples is more than the default value. (default: %(default)s)", default="0.1", type=float, required=False)
    # execute_parse.add_argument("--pause-file", help="Execution will be paused while the file specified by this param exists", type=str, required=False)

    if len(sys.agv)==1:
        parser.print_help(sys.stderr)
        sys.exit(100)

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()