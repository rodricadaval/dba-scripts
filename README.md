# dba-scripts
Scripting with bash, python and ansible for PostgreSQL dbas mainly but also mongodb and mysql extras

# The following packages must be isntalled:
# python > 3.7
# ansible
# ansible-galaxy
# aws cli sdk (https://aws.amazon.com/cli/)
# docker
# docker-compose

# There is a postgresql example client in path repos/python-scripts/dbacli that can run queries and connect to any postgres version via docker containers. 
# This client uses a dynamodb table as a source (python shelve is deprecated due to locking issues), but can be addapted to a different situation.

# Most useful scripts:

#############
## ANSIBLE ##
#############

## In dba-scripts/ansible
# make_global_inventory.py: Create two dynamic inventories for each aws profile given as a parameter based on country tag and profile itself 

## In dba-scripts/ansible/playbooks
# connectivity.yml
# create_read_replica.yml
# custom_query_on_rds.yml
# postgres_integer_reaching_max_per_host.yml
# postgres_reaching_transaction_ids_limit.yml
# rds_bad_backup_setting.yml
# ddl_killer.yml

####################################################
## POSTGRES MONITORING USEFUL QUERIES ##
####################################################

## In dba-scripts/monitoring
# databasefrozenids.sql
# check_table_sizes.sql
# check_unused_indexes.sql
# dead_tuples.sql
# missing_index_2.sql
# pg_show_indexes_invalid.sql
# tablefrozenids.sql
# check_long_queries.sql
# check_locks.sql

####################################################
##### Postgres Instance Scripts (Aws oriented) #####
####################################################

## In dba-scripts/postgresql
# pg-schema-change.py: speed up cloning a table (monitoring instance metrics) safely with parallel workers (also works for transfering defining an SLA).
# pg-archiver.py deleting old rows (monitoring instance metrics) safely.

## In dba-scripts/postgresql/common
# modify-parameter-groups.py: modifyng any parameter in a set of parameter groups by (can filter by tag or dbinstance name)
# rds_maintenance_windows.py: listing maintenance windows for each instance
# suggest_conversion_integer_reaching_limit_per_host.py: detecting integer columns that are near reaching type limit. 
# check_extension_in_profile.py: listing extensions in rds instances
# check-upgrade-incompatible-objects.py: showing possible objects in conflict if in-place major upgrade

####################################################
##### MongoDB/DocumentDB queries and scripts #####
####################################################
## In dba-scripts/mongodb/common
# downscaling_rr.py: performing downscale and upscale in replica instances in non intense hours to save costs
# wrong-master-size.py: alerting when replica node is bigger than master (probably due to an unexpected failover)

## In dba-scripts/mongodb/common/monitoring_queries
# connections.js
# all_running_queries.mongoq
# get_all_long_running_blocked_queries.mongoq

####################################################
##### Useful Docker Containers and DBACLI  #####
####################################################
## In dba-scripts/repos/postgres-dockers
# docker-compose.yml: running each postgresql version from 9.6 to 14

## In dba-scripts/repos/python-scripts
# docker-compose.yml: running each postgresql version from 9.6 to 14