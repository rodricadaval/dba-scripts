import os
import json
import requests
from requests.auth import HTTPBasicAuth

class FivetranObject:

    def __init__(self, api_key, api_secret):
        self.header = {"content-type": "application/json"}
        try:
            # Create Base64 Encoded Basic Auth Header
            self.auth = HTTPBasicAuth(api_key, api_secret)
        except Exception as e:
            print("It was not possible to connect with Fivetran")
            raise(e)

    def get_group_id(self, my_group):
        limit = 1000
        params = {"limit": limit}
        url = "https://api.fivetran.com/v1/groups"
        response = requests.get(url=url, auth=self.auth, params=params).json()
        group_list = response["data"]['items']
        while "next_cursor" in response["data"]:
            params = {"limit": limit, "cursor": response["data"]["next_cursor"]}
            response_paged = requests.get(url=url, auth=self.auth, params=params).json()
            if any(response_paged["data"]["items"]) == True:
                group_list.extend(response_paged["data"]['items'])
                response = response_paged
        json.dumps(group_list)
        group_id = None
        for group in group_list:
            for id, name in group.items():
                if name == my_group:
                    group_id = group['id']
        if group_id is None:
            raise Exception(f"The group {my_group} was not found, try with another group.")
        print("{} Group ID = ".format(my_group) + group_id)
        return group_id

    def get_connector_details(self, group_id):
        limit = 1000
        params = {"limit": limit}
        url = f"https://api.fivetran.com/v1/groups/{group_id}/connectors"
        response = requests.get(url=url, auth=self.auth, params=params).json()
        conn_list = response["data"]['items']
        while "next_cursor" in response["data"]:
            params = {"limit": limit, "cursor": response["data"]["next_cursor"]}
            response_paged = requests.get(url=url, auth=self.auth, params=params).json()
            if any(response_paged["data"]["items"]) == True:
                conn_list.extend(response_paged["data"]['items'])
                response = response_paged
        return conn_list

    def retrieve_connector_schema_config(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas"
        response = requests.get(url=url, 
                                auth=self.auth, 
                                params=params,
                                headers=self.header).json()
        return response

    def reload_connector_schema_config(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas/reload"
        response = requests.post(url=url,
                                  auth=self.auth,
                                  data=json.dumps(params),
                                  headers=self.header).json()
        return response

    def retrieve_connector_sync_history(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/sync-history"
        response = requests.get(url=url, 
                                auth=self.auth, 
                                params=params,
                                headers=self.header).json()
        return response        

    def modify_connector_schema_config(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas"
        response = requests.patch(url=url, 
                                auth=self.auth, 
                                data=json.dumps(params),
                                headers=self.header).json()
        return response

    def test_connector(self, connector_id, params={"trust_certificates": True, "trust_fingerprints": True}):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas"
        response = requests.patch(url=url,
                                  auth=self.auth,
                                  data=json.dumps(params),
                                  headers=self.header).json()
        return response

    def create_connector(self, params):
        url = f"https://api.fivetran.com/v1/connectors"
        response = requests.post(url=url, auth=self.auth, json=params).json()

        return response

    def historical_sync_connector(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/resync"
        response = requests.patch(url=url,
                                  auth=self.auth,
                                  data=json.dumps(params),
                                  headers=self.header).json()
        return response

    def retrieve_connector_details(self, connector_id):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}"
        response = requests.get(url=url, auth=self.auth).json()

        return response     

    def modify_connector_table_config(self, connector_id, schema, table):  
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas/{schema}/tables/{table}"
        response = requests.patch(url=url, 
                                auth=self.auth, 
                                data=json.dumps(params),
                                headers=self.header).json()
        return response        

    def delete_connector(self, connector_id):  
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}"
        response = requests.delete(url=url, auth=self.auth, headers=self.header).json()

        return response            

    def modify_connector(self, connector_id, params):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}"
        response = requests.patch(url=url, 
                                auth=self.auth, 
                                data=json.dumps(params),
                                headers=self.header).json()
        return response              

    def sync_connector_data(self, connector_id):
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/force"
        response = requests.post(url=url, auth=self.auth, json={}).json()

        return response