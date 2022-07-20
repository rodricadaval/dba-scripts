#!/bin/bash

#set -x

_usage() { echo "Usage: dump_multiple_table_in_database.sh [-c <A|B|C|E|P|U|M|DEV|TOOL|xx1>] [-i <ACTUAL_INSTANCE_ALIAS>] [-d <DATABASE_NAME>] [-t {TB_NAME1,TB_NAME2...TB_NAMEN}]" 1>&2; exit 1; }

while getopts "c:i:d:t:" o; do
    case "${o}" in
        c)
            c=${OPTARG^^}
        [ "${c}" == "a" -o "${c}" == "b" -o "${c}" == "c" -o "${c}" == "e" -o "${c}" == "p" -o "${c}" == "u" -o "${c}" == "m" -o "${c}" == "DEV" -o "${c}" == "TOOL" -o "${c}" == "xx1" ] || _usage
            ;;
        i)
            i=${OPTARG}
            ;;
	d)
            d=${OPTARG}
            ;;
        t)
            t=${OPTARG}
            ;;
        *)
            _usage
            ;;
    esac
done

if [[ -z "${c}" || -z "${i}" || -z "${t}" || -z "${d}" ]]; then
    _usage
fi

mkdir -p ~/dba-scripts/global-scripts/DUMPING_RDS_AND_SENDING_TO_DEV
chmod 750 -R ~/dba-scripts/global-scripts/DUMPING_RDS_AND_SENDING_TO_DEV

ROOT_PATH=~/dba-scripts/global-scripts/DUMPING_RDS_AND_SENDING_TO_DEV
COUNTRY_CODE=${c}
LOGS_PATH=${ROOT_PATH}/${COUNTRY_CODE}/LOGS
PRODUCTIVE_RDS_NAME=${i}
PRODUCTIVE_RDS_DATABASE=${d}
PRODUCTIVE_RDS_USER=dba_test_service
HOST_VARIABLE_PATH=/tmp/dumping_host_variable_name.$(date +%Y%m%d%H%M%S).log

read -r -p "Escriba el ALIAS del RDS destino o deje el campo vacio: " response
OUTPUT_RDS_INSTANCE=${response}

read -r -p "Escriba la database del RDS destino o deje el campo vacio: " response
OUTPUT_RDS_DATABASE=${response}

IFS=',' read -ra DBS <<< "${t}"
for i in "${DBS[@]}"; do
    DBS_ARRAY+="'$i',"
    DBS_ARRAY_GREP+="$i|"
done

DBS_COMPLETE_ARRAY=${DBS_ARRAY::-1}
DBS_COMPLETE_ARRAY_GREP=${DBS_ARRAY_GREP::-1}

#echo "${DBS_COMPLETE_ARRAY}"
#echo "\"(${DBS_COMPLETE_ARRAY})\""

PRODUCTIVE_RDS_HOST_TEMP=/tmp/productive_rds_host_migrating_ms.$(date +%Y%m%d%H%M%S).log

PRODUCTIVE_RDS_HOST=$(egrep -ie "^${COUNTRY_CODE}:.*${PRODUCTIVE_RDS_NAME}:.*master" ~/.rds_postgresql_endpoints | cut -d: -f6)

echo; echo "PRODUCTIVE_RDS_HOST=${PRODUCTIVE_RDS_HOST}"

sed 's/://g' ~/dba-scripts/repos/devops-private-statics/ms_hotstart_inventory/${COUNTRY_CODE,,}/postgres_database/db_hosts.yml | grep -B 1 "${PRODUCTIVE_RDS_HOST}" | grep -v login > ${HOST_VARIABLE_PATH}
HOST_VARIABLE_NAME=`cat "${HOST_VARIABLE_PATH}" | tr -d '[:space:]'`

#cut -d "=" -f 2 <<< $(aws_dynamodb_anansiblevar_search.sh -c UY -k id -v db_inventory_postgresdb_X1Xtenders_uy_password)

echo; echo "CREATING RDS VALUES FROM ${COUNTRY_CODE} ${PRODUCTIVE_RDS_NAME}"

mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}
chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}
mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}/LOGS
chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}/LOGS

############################################################################################################
IFS=',' read -ra DBS <<< "${t}"
for i in "${DBS[@]}"; do
    #echo; echo "prepare_dump_restore_with_tables_filter.sh -c \"${COUNTRY_CODE}\" -i \"${PRODUCTIVE_RDS_NAME}\" -d \"${d}\" -n \"${OUTPUT_RDS_INSTANCE}\" -t \"${i}\""
    prepare_dump_restore_with_tables_filter.sh -c "${COUNTRY_CODE}" -i "${PRODUCTIVE_RDS_NAME}" -d "${d}" -r "${OUTPUT_RDS_DATABASE}" -n "${OUTPUT_RDS_INSTANCE}" -t "${i}"
done

rm -f ${PRODUCTIVE_RDS_HOST_TEMP}
rm -f ${HOST_VARIABLE_PATH}
