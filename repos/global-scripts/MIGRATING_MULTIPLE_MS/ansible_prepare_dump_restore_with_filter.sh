#!/bin/bash

#set -x

_usage() { echo "Usage: anansiblevar_prepare_dump_restore_with_filter.sh [-c <A|BB|B|CCC|CC|E|x1|UY|C|xx1|DEV|TOOL|xx1>] [-i <INSTANCE_ALIAS>] [-n NEW_RDS_INSTANCE] [-r <ROLES_BY_COMMA>]" 1>&2; exit 1; }

while getopts "c:i:r:n:" o; do
    case "${o}" in
        c)
            c=${OPTARG^^}
        [ "${c}" == "a" -o "${c}" == "BO" -o "${c}" == "b" -o "${c}" == "c" -o  "${c}" == "cc" -o "${c}" == "e" -o "${c}" == "p" -o "${c}" == "u" -o "${c}" == "c" -o "${c}" == "m" -o "${c}" == "DEV" -o "${c}" == "TOOL" -o "${c}" == "xx1" ]
            ;;
        i)
            i=${OPTARG}
            ;;
	r)
            r=${OPTARG}
            ;;
	n)
            n=${OPTARG}
	    ;;
        *)
            _usage
            ;;
    esac
done

if [[ -z "${c}" ]]; then
    _usage
fi

if [[ -z "${i}" ]]; then
    _usage
fi

if [[ -z "${r}" ]]; then
    _usage
fi

COUNTRY_CODE=${c}
PRODUCTIVE_RDS_NAME=${i}
PRODUCTIVE_RDS_USER=dba_test_service
PRODUCTIVE_RDS_HOST_TEMP=/tmp/productive_rds_host_dumping.$(date +%Y%m%d%H%M%S).log
NEW_RDS_NAME=${n}
NEW_RDS_HOST_TEMP=/tmp/new_rds_host_dumping.$(date +%Y%m%d%H%M%S).log

#cut -d: -f1,2,3,4,5,6,7 ~/.rds_postgresql_endpoints | grep -i "${COUNTRY_CODE}:" | grep -i "${PRODUCTIVE_RDS_NAME}" | cut -d: -f6 > ${PRODUCTIVE_RDS_HOST_TEMP}
PRODUCTIVE_RDS_HOST=$(egrep -ie "^${COUNTRY_CODE}:.*${PRODUCTIVE_RDS_NAME}:.*master" ~/.rds_postgresql_endpoints | cut -d: -f6)


if ! [[ -z "${n}" ]]; then
	cut -d: -f1,2,3,4,5,6,7,9 ~/.rds_postgresql_endpoints | egrep -ie "^${COUNTRY_CODE}:.*${NEW_RDS_NAME}:.*master" | cut -d: -f6 > ${NEW_RDS_HOST_TEMP}
	NEW_RDS_HOST=`cat "${NEW_RDS_HOST_TEMP}" | head -1`
else
	NEW_RDS_HOST="<NEW_RDS_HOST>"
fi

IFS=',' read -ra DBS <<< "${r}"
for i in "${DBS[@]}"; do
   anansiblevar_VARIABLE_USER=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k VALUE -v "$i" | grep "_user" | awk -F\= '{print $1}' | head -1)

anansiblevar_USER=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER}" | awk -F\= '{print $2}' | head -1)
if [[ "${anansiblevar_VARIABLE_USER}" =~ "_username" ]]; then
     anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//username/password}" | grep -v "_url" | awk -F\= '{print $2}')
elif [[ "${anansiblevar_VARIABLE_USER}" =~ "_user" ]]; then
     anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//user/password}" | grep -v "_url" | awk -F\= '{print $2}')
fi
#echo "${anansiblevar_USER}=${anansiblevar_PASSWORD}"

if ! [[ -z "${anansiblevar_VARIABLE_USER}" ]]; then
awspsql.sh -c ${COUNTRY_CODE} -i ${PRODUCTIVE_RDS_NAME} -u ${PRODUCTIVE_RDS_USER} -a <<EOF
SELECT 'pg_dump -v --host="${PRODUCTIVE_RDS_HOST}" --username=${PRODUCTIVE_RDS_USER} "${anansiblevar_USER}" | psql --host="${NEW_RDS_HOST}" --username=${PRODUCTIVE_RDS_USER} "${anansiblevar_USER}"'
FROM pg_roles r
WHERE r.rolname = '${anansiblevar_USER}';
EOF
fi
done
