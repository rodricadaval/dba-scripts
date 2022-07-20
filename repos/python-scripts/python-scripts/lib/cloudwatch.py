import boto3
from .settings import Settings as s
import json
from botocore.exceptions import ClientError
from botocore.config import Config
from datetime import datetime, timedelta
import sys
import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
from pprint import pprint
import logging

logging.basicConfig(level=logging.INFO)

class CloudWatchClient:
    check_interval = 60
    min_burst_balance = 80
    instance_name = ""
    pg_instance_endpoint = ""
    cpu_list = []
    time_passed = []


    __pause_file = "/tmp/cloudwatch_pause.file"
    __aws_profile = ""
    __region = ""

    def __init__(self, payload, log):
        self.logger = log
        self.instance_name = payload.endpoint.split(".")[0]
        self.pg_instance_endpoint = payload.endpoint
        self.__aws_profile = s.AWS_PROFILE[payload.country]
        self.__region = s.REGION[payload.country]
        self.__max_cpu = payload.max_cpu
        self.__max_disk_queue = payload.max_disk_queue
        self.__max_replication_slot_lag = payload.max_replication_slot_lag
        self.max_read_latency = payload.max_read_latency
        self.max_write_latency = payload.max_write_latency
        #if payload.max_value:
        self.__max_value = payload.max_value
        self.__max_value_formatted = payload.max_value_formatted
        self.db_host = payload.db_host
        self.db = payload.db
        self.db_user = payload.db_user
        self.secret = payload.db_secret

    def open_conn(self):
        conn = psycopg2.connect(
            "dbname='{}' host='{}' user='{}' password='{}'"
                .format(self.db, self.db_host, self.db_user, self.secret))
        conn.autocommit = True
        return conn

    def __get_rds_metrics(self):
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
            },
            {
                'Id': 'request4',
                'Label': "ReadLatency",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'ReadLatency',
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
                'Id': 'request5',
                'Label': "WriteLatency",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'WriteLatency',
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
                'Id': 'request6',
                'Label': "OldestReplicationSlotLag",
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'OldestReplicationSlotLag',
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
            region_name = self.__region,
            connect_timeout = 5,
            read_timeout = 60,
            retries = {
                'max_attempts': 50,
                'mode': 'standard'
            }
        )
        session = boto3.session.Session(profile_name=self.__aws_profile)
        cloudwatch = session.client('cloudwatch', config=api_config)

        try:
            # get metrics from the last 5 minutes
            response = cloudwatch.get_metric_data(
                MetricDataQueries = queries['metrics'],
                StartTime = (datetime.now() - timedelta(seconds=60 * 5)).timestamp(),
                EndTime = datetime.now().timestamp(),
            )

            last_cpu_minutes = None
            last_dq_minutes = None
            last_bb_minutes = None
            last_rl_minutes = None
            last_wl_minutes = None
            last_oldest_replication_slot_lag = None

            if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
                if len(response['MetricDataResults'][0]['Values']) > 0:
                    cpu_utilization = round(response['MetricDataResults'][0]['Values'][0], 2)
                    last_cpu_minutes = [round(response['MetricDataResults'][0]['Values'][i], 2)
                                    for i in range(len(response['MetricDataResults'][0]['Values']))]
                else:
                    cpu_utilization = -1

                if len(response['MetricDataResults'][1]['Values']) > 0:
                    disk_queue_depth = round(response['MetricDataResults'][1]['Values'][0], 2)
                    last_dq_minutes = [round(response['MetricDataResults'][1]['Values'][i], 2)
                                        for i in range(len(response['MetricDataResults'][1]['Values']))]
                else:
                    disk_queue_depth = -1

                if len(response['MetricDataResults'][2]['Values']) > 0:
                    burst_balance = round(response['MetricDataResults'][2]['Values'][0], 2)
                    last_bb_minutes = [round(response['MetricDataResults'][2]['Values'][i], 2)
                                       for i in range(len(response['MetricDataResults'][2]['Values']))]
                else:
                    burst_balance = 100

                if len(response['MetricDataResults'][3]['Values']) > 0:
                    read_latency = round(response['MetricDataResults'][3]['Values'][0], 5)
                    last_rl_minutes = [round(response['MetricDataResults'][3]['Values'][i], 5)
                                       for i in range(len(response['MetricDataResults'][3]['Values']))]
                else:
                    read_latency = 0

                if len(response['MetricDataResults'][4]['Values']) > 0:
                    write_latency = round(response['MetricDataResults'][4]['Values'][0], 5)
                    last_wl_minutes = [round(response['MetricDataResults'][4]['Values'][i], 5)
                                       for i in range(len(response['MetricDataResults'][4]['Values']))]
                else:
                    write_latency = 0

                if len(response['MetricDataResults'][5]['Values']) > 0:
                    oldest_replication_slot_lag = round(response['MetricDataResults'][5]['Values'][0], 5)
                    last_oldest_replication_slot_lag = [round(response['MetricDataResults'][5]['Values'][i], 5)
                                       for i in range(len(response['MetricDataResults'][5]['Values']))]
                else:
                    oldest_replication_slot_lag = 0                    

                return {
                    'CPUUtilization': cpu_utilization,
                    'DiskQueueDepth': disk_queue_depth,
                    'BurstBalance': burst_balance,
                    'ReadLatency': read_latency,
                    'WriteLatency': write_latency,
                    'OldestReplicationSlotLag': oldest_replication_slot_lag,
                    'LastCPUMinutes': last_cpu_minutes,
                    'LastDQMinutes': last_dq_minutes,
                    'LastBBMinutes': last_bb_minutes,
                    'LastRLMinutes': last_rl_minutes,
                    'LastWLMinutes': last_wl_minutes,
                    'LastOldestReplicationSlotLag': last_oldest_replication_slot_lag
                    }
            else:
                return {'CPUUtilization': -1,'DiskQueueDepth': -1,'BurstBalance': -1, 'ReadLatency': -1, 'WriteLatency': -1}
        except Exception as e:
            # self.logger.error("AWS API error: %s", e)
            # self.logger.error("AWS API config: %s", api_config)
            # self.logger.error("AWS API MetricDataQueries: %s", queries['metrics'])
            self.logger.error("There was an error trying to read cloudwatch metrics. Check if the country and endpoint are in "
                    "the same aws profile and try again")
            self.logger.error("Error: {}".format(e))
            self.logger.error("Closing program")
            self.logger.warning("Finished")
            sys.exit(5)

    def adjust_wait_time(self, dictio, higher=True) -> float:
        aux = dictio['wait_per_transaction']
        if not dictio['NoLimits']:
            if float(aux) < 0.001 and higher:
                return round(0.01,4)
            elif 0.001 < float(aux) < 60 and higher:
                return round(float(aux) * 2,4)
            elif float(aux) > 60:
                return round(float(60), 4)
            else:
                return round(float(aux) / 2, 4)
        else:
            return round(aux)

    def bytesto(self, bytes, to, bsize=1024): 
        a = {'k' : 1, 'm': 2, 'g' : 3, 't' : 4, 'p' : 5, 'e' : 6 }
        r = float(bytes)
        return bytes / (bsize ** a[to])

    def check_metrics_before_to_continue(self, query, status, event, dictio, loop=True):
        self.time_passed = [((i + 1) * self.check_interval) for i in range(5)]
        while True:
            # percent of CPU and number of disk-queue into the RDS instance
            metrics = self.__get_rds_metrics()
            last_val = 0
            with self.open_conn().cursor() as cursor:
                if loop:
                    if "timestamp" in dictio['type'] or "date" in dictio['type']:
                        cursor.execute(query + "'{}';".format(self.__max_value_formatted))
                    else:
                        cursor.execute(query + '{};'.format(self.__max_value))
                    last_val = cursor.fetchone()[0]
                    if isinstance(last_val, datetime):
                        last_val = str(last_val)
                        if "with time zone" in dictio['type'] and last_val is not None:
                            size = len(last_val)
                            if ":" in last_val[:size - 3] and "-" in last_val[:size - 6]:
                                last_val = last_val[:size - 6]
                            elif ":" not in last_val[:size - 3] and "-" in last_val[:size - 3]:
                                last_val = last_val[:size - 3]
                if last_val is None:
                    last_val = 0
                aux = True
                for worker in dictio.keys():
                    if "worker" in worker:
                        if not dictio[worker]['finished']:
                            aux = False
                if aux and loop:
                    break

            coef = self.check_bullish_resources(metrics, dictio)
            if coef is not None:
                cpu_coef = round(coef[0][0],4)
                dq_coef = round(coef[1][0],4)
                sl_coef = round(coef[2][0], 4)
            else:
                cpu_coef = 0.0000
                dq_coef = 0.0000
                sl_coef = 0.0000

            aux = dictio['wait_per_transaction']
            if float(aux) > 240:
                 dictio['wait_per_transaction'] = round(float(240),4)

            if not dictio['NoLimits']:
                # convert bytes to gigabytes
                OldestReplicationSlotLag = self.bytesto(float(metrics['OldestReplicationSlotLag']), 'g')

                #self.logger.warning("OldestReplicationSlotLag MB: {} - OldestReplicationSlotLag GB: {}".format(metrics['OldestReplicationSlotLag'], OldestReplicationSlotLag))

                if metrics['CPUUtilization'] < self.__max_cpu and metrics['DiskQueueDepth'] < self.__max_disk_queue \
                        and metrics['BurstBalance'] >= self.min_burst_balance and metrics['ReadLatency'] <= \
                        self.max_read_latency and metrics['WriteLatency'] <= self.max_write_latency \
                        and OldestReplicationSlotLag <= self.__max_replication_slot_lag:
                    self.logger.warning("Status: %s  Instance: %s  CPU[%d]: %.2f  DiskQ[%d]: "
                                        "%.2f  Burst Bal[%d]: %.2f  ReplicaSlotLag[%d GB]: %.2f GB  RL/WL[%.1f/%.1f ms]: (%.2f/%.2f)  LastVal: "
                                        "%s  Coef ("
                                        "CPU/DQ/SL): (%.4f/%.4f/%.4f)"
                                        "  Speed: %.4f" % (
                                                            'Processing',
                                                            str(self.instance_name),
                                                            self.__max_cpu,
                                                            metrics['CPUUtilization'],
                                                            self.__max_disk_queue,
                                                            metrics['DiskQueueDepth'],
                                                            self.min_burst_balance,
                                                            metrics['BurstBalance'],
                                                            self.__max_replication_slot_lag,
                                                            OldestReplicationSlotLag,
                                                            round(self.max_read_latency*1000,1),
                                                            round(self.max_write_latency * 1000, 1),
                                                            round(metrics['ReadLatency'] * 1000, 2),
                                                            round(metrics['WriteLatency'] * 1000, 2),
                                                            str(last_val),
                                                            cpu_coef,
                                                            dq_coef,
                                                            sl_coef,
                                                            dictio['wait_per_transaction']
                                                            ))
                    message = ""
                    for each_work in dictio.keys():
                        if "worker" in each_work:
                            aux_val = datetime.fromtimestamp(dictio[each_work]['value']).strftime(
                                                            '%Y-%m-%d %H:%M:%S.%f') if ("timestamp" in dictio[
                                                            'type'] or "date" in dictio['type']) and last_val else str(dictio[each_work]['value'])
                            if ("timestamp" in dictio['type'] or "date" in dictio['type']) and last_val:
                                message = "{}; {}:{}".format(message, each_work, aux_val) if message else \
                                    "{}:{}".format(each_work, aux_val)
                            else:
                                message = "{}; {}:{}".format(message, each_work, aux_val) if message else \
                                      "{}:{}".format(each_work, aux_val)
                    self.logger.warning(message)

                    if metrics['CPUUtilization'] * 2.5 < self.__max_cpu:
                        dictio['wait_per_transaction'] = self.adjust_wait_time(dictio, False)
                    if not event.is_set():
                        event.set()
                    if loop:
                        time.sleep(self.check_interval)
                    else:
                        break
                else:
                    self.logger.warning("Status: %s  Instance: %s  CPU[%d]: %.2f  DiskQ[%d]: "
                                        "%.2f  Burst Bal[%d]: %.2f  ReplicaSlotLag[%d GB]: %.2f GB  RL/WL[%.1f/%.1f ms]: (%.2f/%.2f)  LastVal: "
                                        "%s  Coef ("
                                        "CPU/DQ/SL): ("
                                        "%.4f/%.4f/%.4f)  Speed: "
                                        "%.4f" % (
                                                        'Waiting',
                                                        str(self.instance_name),
                                                        self.__max_cpu,
                                                        metrics['CPUUtilization'],
                                                        self.__max_disk_queue,
                                                        metrics['DiskQueueDepth'],
                                                        self.min_burst_balance,
                                                        metrics['BurstBalance'],
                                                        self.__max_replication_slot_lag,
                                                        OldestReplicationSlotLag,
                                                        round(self.max_read_latency*1000,1),
                                                        round(self.max_write_latency * 1000, 1),
                                                        round(metrics['ReadLatency'] * 1000, 2),
                                                        round(metrics['WriteLatency'] * 1000, 2),
                                                        str(last_val),
                                                        cpu_coef,
                                                        dq_coef,
                                                        sl_coef,
                                                        dictio['wait_per_transaction']
                                                        ))
                    if event.is_set():
                        event.clear()
                        dictio['wait_per_transaction'] = self.adjust_wait_time(dictio)
                    if loop:
                        time.sleep(self.check_interval)
                    else:
                        break
            else:
                self.logger.warning("Instance: %s  CPU[%d]: %.2f  DiskQ[%d]: "
                                    "%.2f  Burst Bal[%d]: %.2f  RL/WL[%.1f/%.1f ms]: (%.2f/%.2f)  LastVal: %s  Coef ("
                                    "CPU/DQ/SL): (%.4f/%.4f/%.4f)"
                                    "  Speed: %.4f" % (
                                        str(self.instance_name),
                                        self.__max_cpu,
                                        99.99,
                                        self.__max_disk_queue,
                                        metrics['DiskQueueDepth'],
                                        self.min_burst_balance,
                                        round(self.max_read_latency*1000,1),
                                        round(self.max_write_latency * 1000, 1),
                                        round(metrics['ReadLatency'] * 1000, 2),
                                        round(metrics['WriteLatency'] * 1000, 2),
                                        99.99,
                                        str(last_val),
                                        cpu_coef,
                                        dq_coef,
                                        sl_coef,
                                        0.0001
                                    ))


    def check_bullish_resources(self, metrics, dictio):
        try:
            if len(self.time_passed) == 0:
                self.time_passed.append(self.check_interval)
            else:
                self.time_passed.append(self.check_interval+self.time_passed[-1])

            if len(self.time_passed) >= 5:
                metrics['LastCPUMinutes'] = metrics['LastCPUMinutes'][::-1]
                metrics['LastDQMinutes'] = metrics['LastDQMinutes'][::-1]
                metrics['LastOldestReplicationSlotLag'] = metrics['LastOldestReplicationSlotLag'][::-1]
                metrics['LastOldestReplicationSlotLag'] = list(map(lambda x: round((x / 1024 / 1024 / 1024), 2), metrics['LastOldestReplicationSlotLag']))

                if len(metrics['LastCPUMinutes']) <= 1 or len(metrics['LastDQMinutes']) <= 1 \
                        or len(metrics['LastOldestReplicationSlotLag']) <= 1:
                    self.logger.warning("Las metricas de cloudwatch CPU, DQ y Slot Lag trajeron pocos datos. Continuo "
                                        "igualmente")
                    return [[0.0000], [0.0000], [0.0000]]

                l1 = metrics['LastCPUMinutes']
                l2 = metrics['LastDQMinutes']
                l3 = metrics['LastOldestReplicationSlotLag']
                s1 = pd.Series(l1, name='LastCPUMinutes')
                s2 = pd.Series(l2, name='LastDQMinutes')
                s3 = pd.Series(l3, name='LastOldestReplicationSlotLag')
                df = pd.concat([s1, s2, s3], axis=1)
                df = pd.DataFrame(df).fillna(0)

                if len(metrics['LastCPUMinutes']) > len(metrics['LastDQMinutes']) \
                        or len(metrics['LastCPUMinutes']) > len(metrics['LastOldestReplicationSlotLag']):
                    metrics['LastCPUMinutes'].pop(0)
                elif len(metrics['LastCPUMinutes']) < len(metrics['LastDQMinutes']):
                    metrics['LastDQMinutes'].pop(0)
                elif len(metrics['LastCPUMinutes']) < len(metrics['LastOldestReplicationSlotLag']):
                    metrics['LastOldestReplicationSlotLag'].pop(0)

                if len(self.time_passed) > len(metrics['LastCPUMinutes']):
                    self.time_passed.pop(0)
                elif len(self.time_passed) < len(metrics['LastCPUMinutes']):
                    self.time_passed.append(self.check_interval+self.time_passed[-1])

                y_values = df[["LastCPUMinutes", "LastDQMinutes", "LastOldestReplicationSlotLag"]]
                x_values = np.aray(self.time_passed).reshape(-1, 1)

                # Create linear regression object
                regr = linear_model.LinearRegression()

                # Train the model using the training sets
                regr.fit(x_values, y_values)

                if (regr.coef_[0][0] > 0.2 and metrics['CPUUtilization'] * 1.2 > float(self.__max_cpu)) or (regr.coef_[1][
                    0] > 0.1 and metrics['DiskQueueDepth'] * 1.2 > float(self.__max_disk_queue)) or (regr.coef_[2][
                    0] > 0.1 and metrics['OldestReplicationSlotLag'] * 1.2 > float(self.__max_replication_slot_lag)):
                    self.logger.warning("Tendencia alcista de consumo de recursos. Aumentando sleep entre transacciones")
                    if not dictio['NoLimits']:
                        dictio['wait_per_transaction'] = self.adjust_wait_time(dictio, True)
                    return regr.coef_
                elif regr.coef_[0][0] < -0.04 and regr.coef_[1][0] < -0.03 and regr.coef_[2][0] < -0.03 \
                        and metrics['CPUUtilization'] * 1.2 < float(self.__max_cpu) \
                        and metrics['DiskQueueDepth'] * 1.2 < float(self.__max_disk_queue) \
                        and metrics['OldestReplicationSlotLag'] * 1.2 < float(self.__max_replication_slot_lag):
                    if not dictio['NoLimits']:
                        if dictio['wait_per_transaction'] > 0.0001:
                            dictio['wait_per_transaction'] = self.adjust_wait_time(dictio, False)
                            self.logger.warning("Tendencia bajista de consumo. Reduciendo sleep entre transacciones")
                elif metrics['CPUUtilization'] * 1.4 < float(self.__max_cpu) \
                        and metrics['DiskQueueDepth'] * 1.5 < float(self.__max_disk_queue) \
                        and metrics['OldestReplicationSlotLag'] * 1.6 < float(self.__max_replication_slot_lag):
                    if not dictio['NoLimits']:
                        if dictio['wait_per_transaction'] > 0.0001:
                            dictio['wait_per_transaction'] = self.adjust_wait_time(dictio, False)
                            self.logger.warning("Bajo de consumo. Reduciendo sleep entre transacciones")
                return regr.coef_
            else:
                return [[0.0000], [0.0000], [0.0000]]
        except ValueError as e:
            self.logger.warning("There is an error with arrays in pandas linear regression: {}".format(e))
        except Exception as e:
            self.logger.warning("There is an error checking if rds resource is bullish: {}".format(e))