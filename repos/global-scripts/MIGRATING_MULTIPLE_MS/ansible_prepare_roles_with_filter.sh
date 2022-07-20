
#!/bin/bash

#set -x

_usage() { echo "Usage: anansiblevar_prepare_roles_with_filter.sh [-c <A|BB|B|CCC|CC|E|x1|UY|C|xx1|DEV|TOOL|xx1>] [-i <INSTANCE_ALIAS>] [-r <ROLES_BY_COMMA>]" 1>&2; exit 1; }

while getopts "c:i:r:" o; do
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
PRODUCTIVE_RDS_HOST_TEMP=/tmp/productive_rds_host_downsizing.$(date +%Y%m%d%H%M%S).log

cut -d: -f1,2,3,4,5,6,7 ~/.rds_postgresql_endpoints | egrep -ie "^${COUNTRY_CODE}:.*${PRODUCTIVE_RDS_NAME}:" | cut -d: -f6 > ${PRODUCTIVE_RDS_HOST_TEMP}

PRODUCTIVE_RDS_HOST=`cat "${PRODUCTIVE_RDS_HOST_TEMP}" | head -1`

IFS=',' read -ra DBS <<< "${r}"
for i in "${DBS[@]}"; do
   anansiblevar_VARIABLE_DATABASE=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k VALUE -v "$i" | egrep "_database" | awk -F\= '{print $1}' | head -1)
#echo "${anansiblevar_VARIABLE_DATABASE}"
   anansiblevar_VARIABLE_USER=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_DATABASE//database/user}" | awk -F\= '{print $1}' | head -1)
#echo "${anansiblevar_VARIABLE_USER}"
   anansiblevar_VARIABLE_DATABASE_1=$(sed 's/pg_//g' <<< "${anansiblevar_VARIABLE_DATABASE}")
#echo "${anansiblevar_VARIABLE_DATABASE_1}"
   anansiblevar_VARIABLE_USER_1=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_DATABASE_1//database/user}" | head -1 | awk -F\= '{print $1}')
#echo "${anansiblevar_VARIABLE_USER_1}"
if [[ "$(echo ${anansiblevar_VARIABLE_USER} | wc -c)" -eq 1 ]]; then
     if [[ "$(echo ${anansiblevar_VARIABLE_USER_1} | wc -c)" -eq 1 ]]; then
     anansiblevar_VARIABLE_USER=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_DATABASE//database/username}" | head -1 | awk -F\= '{print $1}')
     	if [[ "$(echo ${anansiblevar_VARIABLE_USER} | wc -c)" -eq 1 ]]; then
		anansiblevar_VARIABLE_USER_1=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_DATABASE_1//database/username}" | head -1 | awk -F\= '{print $1}')
     	fi
     fi	
fi

if [[ "$(echo ${anansiblevar_VARIABLE_USER} | wc -c)" -eq 1 ]]; then
	anansiblevar_USER=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1}" | awk -F\= '{print $2}' | head -1)
else
	anansiblevar_USER=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER}" | awk -F\= '{print $2}' | head -1)
fi

if [[ "$(echo ${anansiblevar_USER} | wc -c)" -eq 1 ]]; then
    anansiblevar_USER=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1}" | head -1 | awk -F\= '{print $2}')
fi

if [[ "${anansiblevar_VARIABLE_USER}" =~ "_username" ]]; then
     anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//username/password}" | grep -v "_url" | awk -F\= '{print $2}')
     if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1//username/password}" | head -1 | awk -F\= '{print $2}')
     fi
     if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//username/pass}" | head -1 | awk -F\= '{print $2}')
	if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        	anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1//username/pass}" | head -1 | awk -F\= '{print $2}')
     	fi
     fi
     
elif [[ "${anansiblevar_VARIABLE_USER}" =~ "_user" ]]; then
     anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search_exact.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//user/password}" | grep -v "_url" | awk -F\= '{print $2}')
     if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1//user/password}" | head -1 | awk -F\= '{print $2}')
     fi
     if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER//user/pass}" | head -1 | awk -F\= '{print $2}')
	if [[ "$(echo ${anansiblevar_PASSWORD} | wc -c)" -eq 1 ]]; then
        	anansiblevar_PASSWORD=$(aws_dynamodb_anansiblevar_search.sh -c ${COUNTRY_CODE} -k ID -v "${anansiblevar_VARIABLE_USER_1//user/pass}" | head -1 | awk -F\= '{print $2}')
     	fi
     fi
fi

if ! [[ -z "${anansiblevar_VARIABLE_USER}" ]]; then
awspsql.sh -c ${COUNTRY_CODE} -i ${PRODUCTIVE_RDS_NAME} -u ${PRODUCTIVE_RDS_USER} -a <<EOF
SELECT 'ALTER ROLE "${anansiblevar_USER}" WITH PASSWORD ''${anansiblevar_PASSWORD}'';'
FROM pg_roles r
WHERE r.rolname = '${anansiblevar_USER}';
EOF

awspsql.sh -c ${COUNTRY_CODE} -i ${PRODUCTIVE_RDS_NAME} -u ${PRODUCTIVE_RDS_USER} -a <<EOF
SELECT 'ALTER ROLE "${anansiblevar_USER}" SET '||replace(quote_ident(unnest(setconfig)),'=','"="')||';'
FROM pg_roles r
INNER JOIN pg_db_role_setting rs ON rs.setrole = r.oid
LEFT JOIN pg_database d ON d.oid = rs.setdatabase
WHERE r.rolname = '${anansiblevar_USER}';
EOF
fi
done
