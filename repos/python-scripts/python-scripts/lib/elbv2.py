import boto3
from .settings import Settings as s
import json
from botocore.exceptions import ClientError


class ELBClient:

    def __init__(self, country):
        self.country = country
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[country])
        self.elb_client = self.session.client('elbv2', region_name=self.region)

    def get_target_groups(self, target_group_arn):
        return self.elb_client.describe_target_groups(TargetGroupArns=[target_group_arn])['TargetGroups'][0][
            'HealthCheckPath']
