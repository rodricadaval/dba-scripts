from .rds import RDSClient
from .docdb import DocDBClient
from bson import json_util
import json


class Infra:

    def __init__(self, type, country):
        self.country = country
        if type == "ec2":
            self.cli = EC2Client(self.country)
        elif type == "rds":
            self.cli = RDSClient(self.country)
        elif type == "docdb":
            self.cli = DocDBClient(self.country)

    def search_instances(self, instance_name=""):
        return json.dumps(self.cli.search_instances(instance_name), default=json_util.default, indent=4)

    def search_clusters(self, cluster_name):
        return json.dumps(self.cli.search_clusters(cluster_name), default=json_util.default, indent=4)

    def apply_tag_in_instances(self, instance, tag, engine, family=""):
        return json.dumps(self.cli.apply_tag_in_instances(instance, tag, engine, family), default=json_util.default, indent=4)

    def connect(self, name, motor, database, user, secret):
        self.cli.connect(name, motor, database, user, secret)
