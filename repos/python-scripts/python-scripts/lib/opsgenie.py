import boto3
from .settings import Settings as s
import json
from botocore.exceptions import ClientError
import opsgenie_sdk
import logging
from .dynamo import DynamoClient
import sys

# Initialize configurations
conf = opsgenie_sdk.configuration.Configuration()
# Default Logging Settings
conf.logger = {}
conf.logger["package_logger"] = logging.getLogger("opsgenie_client")
conf.logger["urllib3_logger"] = logging.getLogger("urllib3")
# Log format
conf.logger_format = '%(asctime)s %(levelname)s %(message)s'
# Log stream handler
conf.logger_stream_handler = None
# Log file handler
conf.logger_file_handler = None
# Debug file location
conf.logger_file = None
# Debug switch
conf.debug = False

def get_api():
    try:
        i_dynamo = DynamoClient('dev')
        return json.loads(i_dynamo.get_vars('an_opsgenie_api'))['value']
    except Exception as e:
        i_dynamo = DynamoClient('xx')
        return json.loads(i_dynamo.get_vars('an_opsgenie_api'))['value']

class Opsgenie:

    def __init__(self, source="DBA-Python-Scripts"):
        self.api = get_api()
        self.conf = opsgenie_sdk.configuration.Configuration()
        self.conf.api_key['Authorization'] = self.api
        self.api_client = opsgenie_sdk.api_client.ApiClient(configuration=self.conf)
        self.alert_api = opsgenie_sdk.AlertApi(api_client=self.api_client)
        self.source = source
        self.message = ""
        self.description = ""
        self.priority = "P5"
        self.alias = ""
        self.tags = ['RDS', 'postgres', 'dba-monitor', 'integer']
        self.team = "dba"
        self.responders = [{'name': self.team, 'type': 'team'}]
        self.visible = self.responders
        self.actions = ['Need to review by DbaOps', 'Manually fix this issue']
        self.details = {}
        self.entity = 'Data Platform'
        self.last_request_id = 0
        self._alert_id = 0

    def set_message(self, data):
        self.message = data

    def set_description(self, data):
        self.description = data

    def set_priority(self, data):
        self.priority = data

    def set_alias(self, data):
        self.alias = data

    def set_team(self, data):
        self.team = data

    def set_tags(self, data):
        try:
            if not isinstance(data, list):
                logging.error("El listado de tags no es de formato array")
            self.tags = data
        except Exception as e:
            logging.error(e)
            sys.exit(404)

    def set_actions(self, data):
        try:
            if not isinstance(data, list):
                logging.error("Las acciones no son en formato array")
            self.actions = data
        except Exception as e:
            logging.error(e)
            sys.exit(404)

    def set_details(self, data):
        try:
            if not isinstance(data, dict):
                logging.error("Campo details no es en formato dict")
            self.details = data
        except Exception as e:
            logging.error(e)
            sys.exit(404)

    def set_entity(self, data):
        self.entity = data


    def create(self):
        body = opsgenie_sdk.CreateAlertPayload(
            message=self.message,
            alias=self.alias,
            description=self.description,
            responders=self.responders,
            visible_to=self.visible,
            actions=self.actions,
            tags=self.tags,
            details=self.details,
            entity=self.entity,
            priority=self.priority,
            source=self.source
        )

        try:
            create_response = self.alert_api.create_alert(create_alert_payload=body)
            self.last_request_id = create_response.request_id
            return create_response
        except opsgenie_sdk.ApiException as err:
            print("Exception when calling AlertApi->create_alert: %s\n" % err)

    def close_alert(self, id=0, note='Alert resolved automatically'):
        if not id:
            if not self._alert_id:
                response = self.get_request_status(self.last_request_id)
                self._alert_id = response.data.alert_id
        else:
            self._alert_id = id
        body = opsgenie_sdk.CloseAlertPayload(note=note, source=self.source)
        try:
            close_response = self.alert_api.close_alert(identifier=self._alert_id, close_alert_payload=body)
            return close_response
        except opsgenie_sdk.ApiException as err:
            print("Exception when calling AlertApi->close_alerts: %s\n" % err)

    def get_request_status(self, request_id):
        try:
            response = self.alert_api.get_request_status(request_id=request_id)
            return response
        except opsgenie_sdk.ApiException as err:
            print("Exception when calling AlertApi->get request status: %s\n" % err)

    def list_alerts(self, fields='status=open'):
        query = fields
        try:
            list_response = self.alert_api.list_alerts(limit=50, sort='createdAt', order='asc',
                                                       search_identifier_type='name', query=query)
            return list_response
        except ApiException as err:
            print("Exception when calling AlertApi->list_alerts: %s\n" % err)

    def close_listed_alerts(self, fields='tag=RDS and tag=dba-monitor and tag=integer'):
        query = fields
        try:
            list_response = self.alert_api.list_alerts(limit=50, sort='createdAt', order='asc',
                                                       search_identifier_type='name', query=query)
            for alert in list_response.data:
                self.close_alert(alert.id)
        except ApiException as err:
            print("Exception when calling AlertApi->list_alerts: %s\n" % err)

    def add_tags(self, tags_data):
        body = opsgenie_client.AddTagsToAlertPayload(tags=tags_data)
        if not self._alert_id:
            response = self.get_request_status(self.last_request_id)
            self._alert_id = response.data.alert_id
        try:
            add_tags_response = self.alert_api.add_tags(add_tags_to_alert_payload=body, identifier=self._alert_id)
            print(add_tags_response)
            return add_tags_response
        except ApiException as err:
            print("Exception when calling AlertApi->add tags: %s\n" % err)

    def remove_tags(self):
        try:
            if not self._alert_id:
                response = self.get_request_status(self.last_request_id)
                self._alert_id = response.data.alert_id
            remove_tage_response = self.alert_api.remove_tags(identifier=self._alert_id, tags=['OverwriteQuietHours'],
                                                              user='userName', source='python sdk', note='testing')
            print(remove_tage_response)
            return remove_tage_response
        except ApiException as err:
            print("Exception when calling AlertApi->remove tags: %s\n" % err)

    def acknowledge(self, user, note):
        body = opsgenie_client.AcknowledgeAlertPayload(user=user, note=note, source=self.source)
        try:
            if not self._alert_id:
                response = self.get_request_status(self.last_request_id)
                self._alert_id = response.data.alert_id
            awknowledge_response = self.alert_api.acknowledge_alert(identifier=self._alert_id,
                                                                    acknowledge_alert_payload=body)
            print(awknowledge_response)
            return awknowledge_response
        except ApiException as err:
            print("Exception when calling AlertApi->acknowledge: %s\n" % err)