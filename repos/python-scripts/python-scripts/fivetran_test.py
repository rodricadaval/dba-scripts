import pprint
from datetime import datetime
from fivetran import FivetranObject

# -- fivetran
api_key = 's..Q'
api_secret = 'k..H'
my_group = 'S..W'
group_id = 'v...s'
connector_id = ""

five = FivetranObject(api_key, api_secret)
# group_id = five.get_group_id(my_group)

# como buscar un conector con el nombre 
# result = five.get_connector_details(group_id)
# for item in result:
#     if item['service'] in ['mysql', 'aurora', 'postgres_rds'] and item['schema'] == 'xx1_pglr_ms_rpp_onboarding_ms_db':
#         connector_id = item['id']
#         break
        
# print(connector_id)

# crear el conector y esto habilita a todas las tablas a donde el usuario de fivetran este apuntando
# -- mysql
# params = {
#         "service": "aurora",
#         "group_id": group_id,
#         "trust_certificates": True,
#         "trust_fingerprints": True, 
#         "run_setup_tests": True,
#         "paused": True,
#         "config": {
#             "schema_prefix": "random_test_mysql",
#             "host": "cl-unit-aurora.X1X.cl",
#             "port": 3306,
#             "database": "unit-cg-inventory",
#             "user": "fv_role",
#             "password": "ROLESECRET",
#             "tunnel_host": "abastion.acompanydomain.com",
#             "tunnel_port": 22,
#             "tunnel_user": "fivetran",
#             "connection_type": "SshTunnel"
#         }
#     }

# -- postgres
# params = {
#         "service": "postgres_rds",
#         "group_id": group_id,
#         "trust_certificates": True,
#         "trust_fingerprints": True, 
#         "run_setup_tests": True,
#         "paused": True,
#         "config": {
#             "schema_prefix": "random_test_postgres2",
#             "host": "pg-microservices.acompanydomain.com.a",
#             "port": 5432,
#             "database": "application_users",
#             "user": "fv_role",
#             "password": "ROLESECRET",
#             "tunnel_host": "abastion.acompanydomain.com",
#             "tunnel_port": 22,
#             "tunnel_user": "fivetran",
#             "connection_type": "SshTunnel",
#             "update_method": "XMIN",
#             "replication_slot": "test_replication_slot"            
#         }
#     }    
# result = five.create_connector(params)
# pprint.pprint(result)
# {'code': 'Success',
#  'data': {'config': {'connection_type': 'SshTunnel',
#                      'database': 'application_users',
#                      'host': 'pg-microservices.acompanydomain.com.a',
#                      'password': '******',
#                      'port': 5432,
#                      'tunnel_host': 'abastion.acompanydomain.com',
#                      'tunnel_port': 22,
#                      'tunnel_user': 'fivetran',
#                      'update_method': 'XMIN',
#                      'user': 'fv_role'},
#           'connected_by': 'overplay_whence',
#           'created_at': '2021-10-12T16:51:23.471432Z',
#           'failed_at': None,
#           'group_id': 'test',
#           'id': 'fluorescence_scorch',
#           'pause_after_trial': False,
#           'paused': True,
#           'schedule_type': 'auto',
#           'schema': 'random_test_postgres2',
#           'service': 'postgres_rds',
#           'service_version': 3,
#           'setup_tests': [{'message': '',
#                            'status': 'PASSED',
#                            'title': 'Connecting to host'},
#                           {'message': '',
#                            'status': 'PASSED',
#                            'title': 'Connecting to database'},
#                           {'message': "The 'statement_timeout' setting is "
#                                       'valid',
#                            'status': 'PASSED',
#                            'title': "Checking 'statement_timeout' value"},
#                           {'message': 'There is no need to check '
#                                       'wal_sender_timeout configuration when '
#                                       'XMIN replication is selected',
#                            'status': 'SKIPPED',
#                            'title': 'Checking wal_sender_timeout value'},
#                           {'message': 'There is no need to check your database '
#                                       'configuration when XMIN replication is '
#                                       'selected',
#                            'status': 'SKIPPED',
#                            'title': 'Connecting to WAL replication slot'},
#                           {'message': 'This test is only needed when WAL with '
#                                       'pgoutput/publication is selected',
#                            'status': 'SKIPPED',
#                            'title': 'Verifying publication'},
#                           {'message': '',
#                            'status': 'PASSED',
#                            'title': 'Connecting to SSH tunnel'},
#                           {'message': '',
#                            'status': 'PASSED',
#                            'title': 'Validating certificate'}],
#           'status': {'is_historical_sync': True,
#                      'setup_state': 'connected',
#                      'sync_state': 'paused',
#                      'tasks': [],
#                      'update_state': 'on_schedule',
#                      'warnings': []},
#           'succeeded_at': None,
#           'sync_frequency': 360},
#  'message': 'Connector has been created'}



# muestra datos del conector
# result = five.retrieve_connector_details("feathered_materialism")
# pprint.pprint(result)
# 'code': 'Success'

# retornar la config del esquema
# result = five.retrieve_connector_schema_config("feathered_materialism", {})
# pprint.pprint(result['data']['schemas'])
# 'code': 'Success'

# modificar esquema completo (clonar desde otro conector) (conector creado "fluorescence_scorch" como prueba y usamos este conector "feathered_materialism" para copiar la config de los esquemas al nuevo conector)
# result = five.retrieve_connector_schema_config("fluorescence_scorch", {"schemas": result['data']['schemas']})
# pprint.pprint(result)
# 'code': 'Success'

# deshabilitar un esquema en particular
# result = five.retrieve_connector_schema_config("familiar_appetizer", {"schemas": {"sys": {"enabled": False}}})
# pprint.pprint(result)
# 'code': 'Success'

# start sync
# five.sync_connector_data("familiar_appetizer")
# 'code': 'Success'

# elimina un conector
# result = five.delete_connector("familiar_appetizer")
# pprint.pprint(result)
# {'code': 'Success',
#  'message': "Connector with id 'familiar_appetizer' has been deleted"}