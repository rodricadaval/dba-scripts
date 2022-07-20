import boto3
from .settings import Settings as s
import json
import re
#from libs.aws_sso_session import *


class R53Client:

    def __init__(self, country):
        #self.aws = aws_session()
        #self.aws.set_profile(s.AWS_PROFILE[country])
        self.country = country
        self.region = s.REGION[self.country]
        #self.session = boto3.Session(aws_access_key_id=self.aws.aws_access_key_id, aws_secret_access_key=self.aws.aws_secret_access_key, aws_session_token=self.aws.aws_session_token, region_name=self.region)
        self.session = boto3.Session(profile_name=s.AWS_PROFILE[country])
        self.route53_client = self.session.client('route53', region_name=self.region)

    def describe_replication_groups(self, replication_groud_id):
        return self.route53_client.describe_replication_groups(ReplicationGroupId=replication_groud_id)

    def update_record(self, subdomain, new_record):
        print("UPDATING RECORD FOR " + subdomain + " WITH THIS RECORD " + new_record)
        return self.route53_client.change_resource_record_sets(HostedZoneId="XZXXXX",
                                                               ChangeBatch={
                                                                    "Comment": "Automatic DNS update",
                                                                    "Changes": [
                                                                        {
                                                                            "Action": "UPSERT",
                                                                            "ResourceRecordSet": {
                                                                                "Name": subdomain,
                                                                                "Type": "CNAME",
                                                                                "TTL": 300,
                                                                                "ResourceRecords": [
                                                                                    {
                                                                                        "Value": new_record
                                                                                    },
                                                                                ],
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            )

    def delete_record(self, subdomain, record):
        print("DELETING RECORD FOR " + subdomain)
        return self.route53_client.change_resource_record_sets(HostedZoneId="ASDASDASD",
                                                               ChangeBatch={
                                                                    "Comment": "Automatic DNS update",
                                                                    "Changes": [
                                                                        {
                                                                            "Action": "DELETE",
                                                                            "ResourceRecordSet": {
                                                                                "Name": subdomain,
                                                                                "Type": "CNAME",
                                                                                "TTL": 300,
                                                                                "ResourceRecords": [
                                                                                    {
                                                                                        "Value": record
                                                                                    },
                                                                                ],
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            )

    def update_record_weight(self, current_record, weight, new_record, identifier):
        print("UPDATING RECORD FOR " + current_record + " WITH THIS RECORD services_new.dev-k8s.X1Xpay.com")
        return self.route53_client.change_resource_record_sets(HostedZoneId="ASDASDASD",
                                                               ChangeBatch={
                                                                    "Comment": "Automatic DNS update",
                                                                    "Changes": [
                                                                        {
                                                                            "Action": "CREATE",
                                                                            "ResourceRecordSet": {
                                                                                "Name": current_record,
                                                                                "Type": "CNAME",
                                                                                "Weight": weight,
                                                                                "TTL": 300,
                                                                                "SetIdentifier": identifier,
                                                                                "ResourceRecords": [
                                                                                    {
                                                                                        "Value": new_record
                                                                                    },
                                                                                ],
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            )

    def get_record(self, subdomain, current_record):
        print("GETTING OLD RECORD FOR " + subdomain + current_record)
        paginator = self.route53_client.get_paginator('list_resource_record_sets')
        source_zone_records = paginator.paginate(HostedZoneId="KLJALKDFAD")
        for record_set in source_zone_records:
            for record in record_set['ResourceRecordSets']:
                if record['Name'] == subdomain + current_record+".":
                    return record