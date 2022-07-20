from pprint import pprint

import boto3
from .settings import Settings as s
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


class DynamoClient:

    def __init__(self, country):
        self.country = country
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[country])
        self.dynamo_client = self.session.resource('dynamodb', region_name=self.region)
        self.dynamo_table = s.DYNAMO_INVENTORY_TABLE[self.country]

    def get_vars(self, name):

        try:
            response = self.dynamo_client.Table(self.dynamo_table).get_item(
                Key={
                    'id': name,
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            if "Item" in response.keys():
                item = response['Item']
                return json.dumps(item, indent=4)
            else:
                return ""

    def get_values(self, value):

        try:
            data = []
            response = self.dynamo_client.Table(self.dynamo_table).scan(FilterExpression=Attr('value').eq(value))
            if response:
                if "Items" in response.keys():
                    data.extend(response['Items'])
                while 'LastEvaluatedKey' in response:
                        response = self.dynamo_client.Table(self.dynamo_table)\
                            .scan(ExclusiveStartKey=response['LastEvaluatedKey'],
                                  FilterExpression=Attr('value').eq(value))
                        if response:
                            if "Items" in response.keys():
                                data.extend(response['Items'])
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(data, indent=4)

    def update_vars(self, name, new_value):
        try:
            response = self.dynamo_client.Table(self.dynamo_table).put_item(
                Item={
                    'id': name,
                    'value': new_value,
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(response, indent=4)

    def delete_vars(self, name):

        try:
            response = self.dynamo_client.Table(self.dynamo_table).delete_item(
                Key={
                    'id': name,
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(response, indent=4)

    def search_vars(self, expr):
        try:
            response = self.dynamo_client.Table(self.dynamo_table).scan(FilterExpression=Attr('id').contains(expr))
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            #item = response['Item]
            return json.dumps(response["Items"], indent=4)

    def search_values(self, expr):
        try:
            data = []
            response = self.dynamo_client.Table(self.dynamo_table).scan(FilterExpression=Attr('value').contains(expr))
            if response:
                if "Items" in response.keys():
                    data.extend(response['Items'])
                while 'LastEvaluatedKey' in response:
                        response = self.dynamo_client.Table(self.dynamo_table)\
                            .scan(ExclusiveStartKey=response['LastEvaluatedKey'],
                                  FilterExpression=Attr('value').contains(expr))
                        if response:
                            if "Items" in response.keys():
                                data.extend(response['Items'])
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(data, indent=4)
