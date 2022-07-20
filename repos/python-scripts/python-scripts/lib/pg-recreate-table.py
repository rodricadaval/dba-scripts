#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform replace table to new one reducing archiving old rows and converting columns to biginteger.

import os
import datetime
import subprocess
import psycopg2
from pprint import pprint
import sys
import argparse
import shelve
import time
import re
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
from settings import Settings as global_settings
import logging
from logging.handlers import TimedRotatingFileHandler
from subprocess import Popen, PIPE
import shlex
import urllib.parse

logging.basicConfig(level=logging.INFO)

my_logger = logging.getLogger()

class TableCreator:

    def __init__(self, payload):
        self.logger = my_logger
        if "max_value" in payload.keys():
            self.__max_value = payload['max_value']
        else:
            self.__max_value = 0
        self.db_host = payload['db_host']
        self.db = payload['db']
        self.db_user = payload['db_user']
        self.secret = payload['secret']
        self.db_schema = payload['db_schema']
        self.db_schema_formatted = payload['db_schema'].replace("-","_")
        self.db_table = payload['db_table']
        self.db_table_formatted = payload['db_table'].replace("-", "_")
        self.conn = None
        self.statements = dict()
        self.statements['set'] = []
        self.statements['create_table'] = []
        self.statements['alter_table'] = []
        self.statements['create_sequence'] = []
        self.statements['alter_sequence'] = []
        self.statements['create_index'] = []
        self.statements['create_trigger'] = []
        self.statements['grant'] = []
        self.new_table_name = payload['new_table_name']
        self.partition = payload['partition']
        if self.partition:
            self.__partition_sentence = " PARTITION BY RANGE (\"{}\")".format(self.partition['part_column'])
        self.conn = None
        self.postgres_conn = None
        self.uncommited_conn = None

    def open_conn(self):
        try:
            if self.conn:
                if self.conn.closed:
                    conn = psycopg2.connect(
                        "dbname='{}' host='{}' user='{}' password='{}'"
                            .format(self.db, self.db_host , self.db_user, self.secret))
                    conn.autocommit = True
            else:
                conn = psycopg2.connect(
                    "dbname='{}' host='{}' user='{}' password='{}'"
                        .format(self.db, self.db_host, self.db_user, self.secret))
                conn.autocommit = True
            if "conn" in locals():
                self.conn = conn
        except psycopg2.OperationalError as e:
            my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
            sys.exit(500)

    def open_uncommited_conn(self):
        try:
            if self.uncommited_conn:
                if self.uncommited_conn.closed:
                    conn = psycopg2.connect(
                        "dbname='{}' host='{}' user='{}' password='{}'"
                            .format(self.db, self.db_host , self.db_user, self.secret))
                    conn.autocommit = False
            else:
                conn = psycopg2.connect(
                    "dbname='{}' host='{}' user='{}' password='{}'"
                        .format(self.db, self.db_host, self.db_user, self.secret))
                conn.autocommit = False
            if "conn" in locals():
                self.uncommited_conn = conn
        except psycopg2.OperationalError as e:
            my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
            sys.exit(500)

    def open_postgres_conn(self):
        if self.postgres_conn:
            if self.postgres_conn.closed:
                conn = psycopg2.connect(
                    "dbname='postgres' host='{}' user='{}' password='{}'"
                        .format(self.db_host , self.db_user, self.secret))
                conn.autocommit = True
        else:
            conn = psycopg2.connect(
                "dbname='postgres' host='{}' user='{}' password='{}'"
                    .format(self.db_host, self.db_user, self.secret))
            conn.autocommit = True
        if "conn" in locals():
            self.postgres_conn = conn

    def get_master_max_value(self) -> int:
        try:
            if max_value != 0:
                return max_value
            with self.conn.cursor() as cur:
                cur.execute(__qry_master_last_value)
                result = cur.fetchone()
            if result[0] is not None:
                return result[0]
            else:
                raise psycopg2.ProgrammingError("Error al consultar el max column value: {}".format(result[0]))
        except psycopg2.OperationalError as e:
            my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
            sys.exit(500)

    def get_master_min_value(self) -> int:
        try:
            self.open_conn()
            with self.conn.cursor() as cur:
                cur.execute(__qry_master_min_value)
                result = cur.fetchone()
            if result[0] is not None:
                return result[0]
            else:
                raise psycopg2.ProgrammingError("Error al consultar el min column value: {}".format(result[0]))
        except psycopg2.OperationalError as e:
            my_logger.error("Sin conectividad al RDS. Detalle: {}".format(e))
            sys.exit(500)

    def check_secret(self):
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

    def add_foreign_keys(self, create = True):
        self.open_conn()
        with self.conn.cursor() as cursor:
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

    def dump_table(self):
        f_result_name = "/tmp/{}.{}.{}.dmp".format(self.db, self.db_schema_formatted, self.db_table_formatted)
        ############## PRUEBAS, ELIMINAR ####################
        with open(f_result_name, 'r+') as file:
            self.parse_statements(file.readlines())

    def organize_sets(self, line):
        self.statements['set'].append(line.replace("\n",""))

    def organize_grants(self, line):
        self.statements['grant'].append(line.replace("\n",""))

    def organize_ddls(self, position, lines, type):
        self.statements[type].append("")

        long = len(self.statements[type])
        if long == 0:
            last_pos = 0
        else:
            last_pos = long - 1

        while not str(lines[position]).endswith(";\n"):
            if lines[position] != "\n":
                self.statements[type][last_pos] = self.statements[type][last_pos] + lines[position].replace("\n","")
            position = position + 1
        if str(lines[position]).endswith(";\n"):
            if type == "create_table" and self.partition:
                self.statements[type][last_pos] = self.statements[type][last_pos] + \
                                                  lines[position].replace(";\n", self.__partition_sentence + ";")
            elif type == "alter_table" and self.partition and "PRIMARY KEY" in str(lines[position]):
                if self.partition['main_column'] != self.partition['part_column']:
                    lines[position] = re.sub("(.*)PRIMARY KEY.*", r'\1 PRIMARY KEY ("{}", "{}");'.
                                             format(self.partition['main_column'], self.partition['part_column']),
                                             lines[position])
                else:
                    lines[position] = re.sub("(.*)PRIMARY KEY.*", r'\1 PRIMARY KEY ("{}");'.
                                             format(self.partition['main_column']), lines[position])
                self.statements[type][last_pos] = self.statements[type][last_pos] + lines[position].replace("\n",
                                                                                                                "")
            else:
                self.statements[type][last_pos] = self.statements[type][last_pos] + lines[position].replace("\n","")
        self.statements[type][last_pos] = self.statements[type][last_pos].replace(self.db_table, self.new_table_name)
        self.statements[type][last_pos] = self.statements[type][last_pos].replace("ALTER TABLE ONLY", "ALTER TABLE")
        self.statements[type][last_pos] = self.statements[type][last_pos].replace("CREATE TABLE", "CREATE TABLE IF "
                                                                                                  "NOT EXISTS")
        self.statements[type][last_pos] = self.statements[type][last_pos].replace("CREATE SEQUENCE", "CREATE SEQUENCE "
                                                                                                     "IF NOT EXISTS")


    def parse_statements(self, lines):
        i = 0
        while i < len(lines):
            #lines[i] = lines[i].replace(self.db_table, self.new_table_name, 1)
            #aux = lines[i]
            #lines[i] = re.sub(r"{}".format(self.db_table), self.new_table_name, aux)

            if not lines[i].startswith('-- '):
                if lines[i].startswith('SET '):
                    self.organize_sets(lines[i])
                elif lines[i].startswith('CREATE TABLE '):
                    self.organize_ddls(i, lines, 'create_table')
                elif lines[i].startswith('ALTER TABLE '):
                    self.organize_ddls(i, lines, 'alter_table')
                elif lines[i].startswith('CREATE SEQUENCE '):
                    self.organize_ddls(i, lines, 'create_sequence')
                elif lines[i].startswith('ALTER SEQUENCE '):
                    self.organize_ddls(i, lines, 'alter_sequence')
                elif lines[i].startswith('CREATE INDEX '):
                    self.organize_ddls(i, lines, 'create_index')
                elif lines[i].startswith('CREATE TRIGGER '):
                    self.organize_ddls(i, lines, 'create_trigger')
                elif lines[i].startswith('GRANT '):
                    self.organize_grants(lines[i])
            i = i + 1

    def show_create_table(self):
        self.dump_table()
        pprint(self.statements)

    def huge_table(self, analyzed = False):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT ut.n_live_tup FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                           "'{}';".format(self.db_schema, self.db_table))
            result = cursor.fetchone()
            if result is not None:
                # if int(result[0]) > 250000000:
                if int(result[0]) == 0 and not analyzed:
                    cursor.execute(
                        "ANALYZE \"{}\".\"{}\";".format(self.db_schema, self.db_table))
                    return self.huge_table(True)
                elif int(result[0]) > 10000000:
                    return True
            return False

    def check_new_table_existence(self):
        try:
            self.open_conn()
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_stat_user_tables ut WHERE schemaname = '{}' AND relname = "
                               "'{}';".format(self.db_schema, self.new_table_name))
                result = cursor.fetchone()
                if result is not None:
                    my_logger.warning("Table {} was already created.".format(self.new_table_name))
                    return True
                else:
                    return False
        except Exception as e:
            if "pg_stat_statements" in str(e):
                with self.conn.cursor() as cursor:
                    my_logger.warning("Creating pg_stat_statements extension")
                    cursor.execute("CREATE EXTENSION pg_stat_statements;")
                return self.check_new_table_existence()
            else:
                my_logger.error("Error al chequear existencia de la tabla clonada: {}".format(e))
                sys.exit(501)

    def create_new_table_schema(self):
        try:
            self.open_conn()
            with self.conn.cursor() as cursor:
                if not self.check_new_table_existence():
                    my_logger.warning("Setting up session stats")
                    for each_set in self.statements['set']:
                        cursor.execute(each_set)
                    my_logger.warning("Creating table")
                    for each_create in self.statements['create_table']:
                        cursor.execute(each_create)
                    for each_create in self.statements['create_sequence']:
                        cursor.execute(each_create)
                    for each_alter in self.statements['alter_table']:
                        cursor.execute(each_alter)
                    for each_alter in self.statements['alter_sequence']:
                        cursor.execute(each_alter)
                    for each_grant in self.statements['grant']:
                        cursor.execute(each_grant)
                if self.huge_table():
                    cursor.execute("SELECT table_schema, table_name, column_name, data_type FROM "
                                   "information_schema.columns WHERE "
                                   "table_schema = '{}' "
                                   "AND table_name = "
                                   "'{}' AND data_type = 'integer' ORDER BY ordinal_position;".format(self.db_schema,
                                                                                                      self.db_table))
                    result = cursor.fetchall()
                    if result is not None:
                        for each_row in result:
                            my_logger.warning("Changing column " + str(each_row) + " to bigint")
                            cursor.execute("ALTER TABLE \"{0}\".\"{1}\" ALTER COLUMN \"{2}\" TYPE bigint;".format(
                                db_schema,db_table_cloned,str(each_row[2])))
            #add_foreign_keys()
        except Exception as e:
            if "already exists" in str(e):
                my_logger.error("Objeto duplicado. Detalle del error: {}".format(e))
            else:
                my_logger.error("Error al crear nueva tabla: {}".format(e))
            sys.exit(500)

    def drop_cloned_table(self):
        try:
            check_blocked_hours()
            self.open_conn()
            with self.conn.cursor() as cursor:
                my_logger.warning("Dropping table \"{0}\".\"{1}\";".format(db_schema, db_table_cloned))
                cursor.execute("DROP TABLE \"{0}\".\"{1}\";".format(db_schema, db_table_cloned))
            my_logger.warning("Done")
        except Exception as e:
            my_logger.error("Error in dropping cloned table:" + str(e))
            sys.exit(1)

    def get_table_sequences(self, old = True):
        try:
            self.open_conn()
            with self.conn.cursor() as cursor:
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

    def create_partman(self):
        try:
            self.open_conn()
            with self.conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS partman;")
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;")
        except Exception as e:
            pprint("Error creating partman extension: " + str(e))
            sys.exit(1)

    def add_pg_cron(self):
        try:
            self.open_postgres_conn()
            with self.postgres_conn.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")
                __qry_existent_cron_job = "SELECT max(jobid) FROM cron.job WHERE database = '{}';".format(self.db)
                cur.execute(__qry_existent_cron_job)
                result = cur.fetchone()
                if result[0] is None:
                    schedule = """SELECT cron.schedule('@daily', $$CALL partman.run_maintenance_proc()$$);"""
                    cursor.execute(schedule)
                    __qry_last_cron_job = 'SELECT max(jobid) FROM cron.job;'
                    cur.execute(__qry_last_cron_job)
                    result = cur.fetchone()
                if result[0] is not None:
                    cur.execute("UPDATE cron.job SET database = '{}' WHERE jobid = {};".format(self.db, result[0]))
                else:
                    raise psycopg2.ProgrammingError("Error asignar el jobid a la db: {}".format(result[0]))
        except Exception as e:
            my_logger.error("Error creating cron job:" + str(e))
            sys.exit(1)

    def create_parent_table(self):
        try:
            self.open_uncommited_conn()
            with self.uncommited_conn.cursor() as cursor:
                owner = self.get_table_owner()
                cursor.execute("GRANT ALL ON SCHEMA partman TO \"{}\";".format(owner))
                cursor.execute("GRANT ALL ON ALL TABLES IN SCHEMA partman TO \"{}\";".format(owner))
                cursor.execute("SET ROLE \"{}\"".format(owner))
                cursor.execute("SELECT * FROM partman.part_config WHERE parent_table = '{}.{}';".format(self.db_schema,
                                                                                                 self.new_table_name))
                result = cursor.fetchone()
                if result is None:
                    __qry = """
                            SELECT partman.create_parent( p_parent_table => '{}.{}',
                            p_control => '{}',
                            p_type => 'native',
                            p_interval=> '{}',
                            p_premake => {},
                            p_start_partition => '{}');
                            """.format(self.db_schema, self.new_table_name, self.partition['part_column'], self.partition['p_interval'],
                                    self.partition['p_premake'], self.partition['p_start_partition'])
                    cursor.execute(__qry)
                    if self.partition['retention']:
                        __qrt_retention = """
                        UPDATE partman.part_config 
                        SET infinite_time_partitions = true,
                        retention = '{} months', 
                        retention_keep_table={}
                        WHERE parent_table = '{}.{}';""".format(self.partition['retention'],
                                                                self.partition['retention_keep_table'],
                                                                self.db_schema, self.new_table_name)
                        cursor.execute(__qrt_retention)
                else:
                    my_logger.info("PG_PARTMAN parent table already created")
            self.uncommited_conn.commit()
            self.uncommited_conn.close()
        except Exception as e:
            pprint("Error creating partman partition table:" + str(e))
            sys.exit(1)

    def get_table_owner(self):
        self.open_conn()
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT tableowner FROM pg_tables WHERE schemaname = '{}' AND tablename = "
                           "'{}';".format(self.db_schema, self.db_table))
            result = cursor.fetchone()
        return result[0]

    def check_blocked_hours(self):
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

    def begin_creation(self):
        #self.show_create_table()
        #sys.exit(1)
        self.dump_table()
        self.create_new_table_schema()
        self.create_partman()
        self.create_parent_table()
        self.add_pg_cron()


if __name__ == '__main__':
    payload = {
        "max_value": 0,
        "db_host": "pg_microservices.dev.acompanydomain.com",
        "db_user": "dba_test_service",
        "secret": "IAnJdTGrDt9sIn%gPY54!i8g",
        "db": "dispatch",
        "db_schema": "courier_dispatcher",
        "db_table": "order_iterations",
        "new_table_name": "order_iterations_cloned",
        "partition": {"main_column": "id",
                      "part_column": "created_at",
                      "p_interval":"weekly",
                      "p_premake":"5",
                      "p_start_partition": "2021-01-01",
                      "retention": 6,
                      "retention_keep_table": "false"}
        }
    creator = TableCreator(payload)
    creator.begin_creation()
