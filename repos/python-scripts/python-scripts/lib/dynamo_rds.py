from pprint import pprint

import boto3
from .settings import Settings as s
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


class DynamoRDSClient:

    def __init__(self, country, profile=""):
        if str(country).lower() in ["global", "tool"] and "dev" not in profile:
            self.country = "c"
        elif str(country).lower() in ["global", "tool", "c"] and "dev" in profile:
            self.country = "dev"
        else:
            self.country = str(country).lower()
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[country])
        self.dynamo_client = self.session.resource('dynamodb', region_name=self.region)
        self.dynamo_table = s.DYNAMO_INVENTORY_RDS[self.country]

    def get_vars(self, name):

        try:
            response = self.dynamo_client.Table(self.dynamo_table).get_item(
                Key={
                    'id': name,
                }
            )
        except ClientError as e:
            pprint(name)
            print(e.response['Error']['Message'])
        else:
            if "Item" in response.keys():
                item = response['Item']
                return item
            else:
                return ""

    def get_all(self):
        try:
            table = self.dynamo_client.Table(self.dynamo_table)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return table.scan()

    def get_all_ids(self):
        try:
            table = self.dynamo_client.Table(self.dynamo_table)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return table.scan(ProjectionExpression="#id",
                              ExpressionAttributeNames={"#id": "id"})
            # Expression Attribute Names for Projection Expression only.
            return table.scan(ExpressionAttributeNames=["id"])

    def get_values(self, value):

        try:
            response = self.dynamo_client.Table(self.dynamo_table).scan(FilterExpression=Attr('value').eq(value))
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(response["Items"], indent=4)

    def update_vars(self, name, new_value):
        try:
            response = self.dynamo_client.Table(self.dynamo_table).put_item(
                Item={
                    "id": name,
                    "value": new_value,
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
            return json.dumps(response["Items"], indent=4)

    def search_values(self, expr):
        try:
            response = self.dynamo_client.Table(self.dynamo_table).scan(FilterExpression=Attr('value').contains(expr))
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return json.dumps(response["Items"], indent=4)
