#!/bin/bash

#set -x

. ~/.colors

_usage() { echo "Usage: migrating_ms_from_instance_to_existing_instance_pg12_v2.sh [-c <A|B|CCC|CC|E|x1|UY|C|xx1|DEV|TOOL|xx1>] [-k {MS_NAME1,MS_NAME2...MS_NAMEN}]" 1>&2; exit 1; }

while getopts "c:k:" o; do
    case "${o}" in
        c)
            c=${OPTARG^^}
        [ "${c}" == "a" -o "${c}" == "b" -o "${c}" == "c" -o  "${c}" == "cc" -o "${c}" == "e" -o "${c}" == "p" -o "${c}" == "u" -o "${c}" == "c" -o "${c}" == "m" -o "${c}" == "DEV" -o "${c}" == "TOOL" -o "${c}" == "xx1" ] || _usage
            ;;
        k)
            k=${OPTARG}
            k=$(echo "${k}" | sed 's/\s//g')            
            ;;
        *)
            _usage
            ;;
    esac
done

if [[ -z "${c}" || -z "${k}" ]]; then
    _usage
fi

# INSTANCE TO RESIZE X1Xtenderos-u
mkdir -p ~/dba-scripts/global-scripts/MIGRATING_MULTIPLE_MS
sudo chmod 750 -R ~/dba-scripts/global-scripts/MIGRATING_MULTIPLE_MS

ROOT_PATH=./
COUNTRY_CODE=${c}
LOGS_PATH=${ROOT_PATH}/${COUNTRY_CODE}/LOGS
PRODUCTIVE_RDS_USER=dba_test_service
HOST_VARIABLE_PATH=/tmp/host_variable_name.$(date +%Y%m%d%H%M%S).log
MS_REQUIREMENTS_PATH=~/dba-scripts/repos
TEMP_VARS=/tmp/temp_vars.$(date +%Y%m%d%H%M%S).log
JSON_ARRANGEMENT=/tmp/JSON_ARRANGEMENT.$(date +%Y%m%d%H%M%S).log
MS_anansiblevar_VAR=/tmp/MS_anansiblevar_VAR.$(date +%Y%m%d%H%M%S).log

read -r -p "Escriba el ALIAS del RDS destino o deje el campo vacio: " response
NEW_RDS_INSTANCE=${response}

IFS=',' read -ra DBS <<< "${k}"
for i in "${DBS[@]}"; do
    DBS_ARRAY+="'$i',"
    DBS_ARRAY_GREP+="$i|"
done

DBS_COMPLETE_ARRAY=${DBS_ARRAY::-1}
DBS_COMPLETE_ARRAY_GREP=${DBS_ARRAY_GREP::-1}
MS_INPUT_W_SPACES=$(echo "${k}" | tr , ' ')

if [[ $( echo "${NEW_RDS_INSTANCE}" | wc -c ) > 2 ]]; then
        NEW_RDS_HOST=$(cut -d: -f1,2,3,4,5,6,7,9 ~/.rds_postgresql_endpoints | egrep -ie "^${COUNTRY_CODE}:.*${NEW_RDS_INSTANCE}:.*master" | cut -d: -f6 | head -1)
else
        NEW_RDS_HOST="<NEW_RDS_HOST>"
fi

~/dba-scripts/global-scripts/get_ms_data_from_requirements.sh -c $COUNTRY_CODE -m "${k}" -s > ${JSON_ARRANGEMENT}

#jq --ag micro "$ms" --agjson msdata "$(<${JSON_RESULT_FINAL})" '.[$micro] += $msdata'

#jq --ag country "${COUNTRY_CODE}" '.[$country]' ${JSON_ARRANGEMENT} | jq --ag ms "${k}" '.[$ms].postgres.original | keys | join(" ")' | sed 's/\"//g'

MS_LIST_W_SPACES=$(jq --ag country "${COUNTRY_CODE}" '.[$country] | keys | join(" ")' ${JSON_ARRANGEMENT} | sed 's/\"//g')

echo ""

mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}
mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}/LOGS

chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}
chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}/LOGS

rm -f ${LOGS_PATH}/individual_target_creation.log
rm -f ${LOGS_PATH}/target_creation.log
rm -f ${LOGS_PATH}/individual_dump_sentences.log
rm -f ${LOGS_PATH}/dump_sentences.log
rm -f ${LOGS_PATH}/individual_python_vars.log
rm -f ${LOGS_PATH}/python_vars.log
rm -f ${LOGS_PATH}/individual_role_connection_limit.log
rm -f ${LOGS_PATH}/role_connection_limit.log
rm -f ${LOGS_PATH}/individual_role_disable_connection_limit.log
rm -f ${LOGS_PATH}/role_disable_connection_limit.log
rm -f ${LOGS_PATH}/individual_alter_database_name.log
rm -f ${LOGS_PATH}/alter_database_name.log
rm -f ${LOGS_PATH}/individual_killing_connections.log
rm -f ${LOGS_PATH}/killing_connections.log
rm -f ${LOGS_PATH}/individual_role_creation.log
rm -f ${LOGS_PATH}/role_creation.log

chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}
chmod 750 -R ${ROOT_PATH}/${COUNTRY_CODE}/LOGS

touch ${LOGS_PATH}/individual_target_creation.log
touch ${LOGS_PATH}/target_creation.log
touch ${LOGS_PATH}/individual_dump_sentences.log
touch ${LOGS_PATH}/dump_sentences.log
touch ${LOGS_PATH}/individual_python_vars.log
touch ${LOGS_PATH}/python_vars.log
touch ${LOGS_PATH}/individual_role_connection_limit.log
touch ${LOGS_PATH}/role_connection_limit.log
touch ${LOGS_PATH}/individual_role_disable_connection_limit.log
touch ${LOGS_PATH}/role_disable_connection_limit.log
touch ${LOGS_PATH}/individual_alter_database_name.log
touch ${LOGS_PATH}/alter_database_name.log
touch ${LOGS_PATH}/individual_killing_connections.log
touch ${LOGS_PATH}/killing_connections.log
touch ${LOGS_PATH}/individual_role_creation.log
touch ${LOGS_PATH}/role_creation.log

######################## CREACION DE ROLES ADMIN  ############################

echo; echo -e "${BIWhite}----------------------FIXING PERMISSIONS------------------------${No_Color}"; echo

    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_admin_roles.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_global_views_pg12.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_admin_roles_settings.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_grant_roles_devopsadmin.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_public_grants.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_admin_roles_tools_settings.sql"
    echo "awspsql.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -f ~/dba-scripts/postgresql/common/pg_fix_grant_roles.sql"

echo; echo -e "${BIWhite}--------------------END FIXING PERMISSIONS---------------------${No_Color}"; echo

############################################################################################################

echo -e "${BICyan}CHECKING INPUT MICROSERVICES EXISTANCE IN: ${COUNTRY_CODE} ${No_Color}"; echo

IFS=' ' read -a ms_array <<< "$MS_LIST_W_SPACES"
IFS=' ' read -a ms_input_array <<< "$MS_INPUT_W_SPACES"

for i in "${ms_input_array[@]}"; do
	if ! [[ " ${ms_array[@]} " =~ " ${i} " ]]; then
		echo -e "${BIRed}Microservice - $i - does not exists in ${COUNTRY_CODE}. Will be omitted ${No_Color}"; echo
	fi
done

echo -e "${BICyan}CHECKING FINISHED ${No_Color}"; echo

echo; echo -e "${BIWhite}--------------------STARTING MIGRATION---------------------${No_Color}"; echo

for ms in $MS_LIST_W_SPACES; do
	#echo "cat ${JSON_ARRANGEMENT} | jq --ag var_ms \"$ms\" \'.[$var_ms].vars_ansible.anansiblevar_database_host_var\'"
	#search=".\"$ms\".vars_ansible.anansiblevar_database_host_var"
	
	anansiblevar_HOST=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original.vars_ansible.anansiblevar_database_host_var'' | head -1 | sed 's/\"//g')
	anansiblevar_DATABASE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original.vars_ansible.anansiblevar_database_name_var'' | head -1 | sed 's/\"//g')
	anansiblevar_USER=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original.vars_ansible.anansiblevar_database_user_var'' | head -1 | sed 's/\"//g')
	anansiblevar_PASSWORD=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original.vars_ansible.anansiblevar_database_password_var'' | head -1 | sed 's/\"//g')
	anansiblevar_PASSWORD_URL=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original.vars_ansible.anansiblevar_database_password_var_url'' | head -1 | sed 's/\"//g')
	anansiblevar_HOST_VALUE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original'' | jq -r ''."$anansiblevar_HOST"'' | head -1)
	anansiblevar_DATABASE_VALUE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original'' | jq -r ''."$anansiblevar_DATABASE"'' | head -1)
	anansiblevar_USER_VALUE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original'' | jq -r ''."$anansiblevar_USER"'' | head -1)
	anansiblevar_PASSWORD_VALUE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original'' | jq -r ''."$anansiblevar_PASSWORD"'' | head -1)
	anansiblevar_PASSWORD_URL_VALUE=$(cat ${JSON_ARRANGEMENT} | jq ''.\"${COUNTRY_CODE}\".\"$ms\".postgres.original'' | jq -r ''."$anansiblevar_PASSWORD_URL"'' | head -1)
	
   if [[ $NEW_RDS_HOST != $anansiblevar_HOST_VALUE ]]; then
	######################## CREACION DE ROLES OWNER Y DATABASES ############################
	echo; echo -e "${BIWhite}MIGRATION OVER: ${ms^^} ${No_Color}"; echo
	
	#CREACION DEL ROLE
	echo -e "${BIRed}CREATING USER: $anansiblevar_USER_VALUE in $NEW_RDS_HOST ${No_Color}"; echo
	echo "awspsql_create_user_with_country.sh -c ${COUNTRY_CODE} -i ${NEW_RDS_INSTANCE} -u ${PRODUCTIVE_RDS_USER} -d postgres -n $anansiblevar_USER_VALUE -r writeallaccess" > ${LOGS_PATH}/individual_role_creation.log
	cat ${LOGS_PATH}/individual_role_creation.log
	echo ""
	cat ${LOGS_PATH}/individual_role_creation.log >> ${LOGS_PATH}/role_creation.log

	#CREACION DE DB
	#echo -e "${BICyan}CREATING DATABASE... ${No_Color}"; echo
	echo "psql -t --host=$NEW_RDS_HOST -U $PRODUCTIVE_RDS_USER -d postgres <<EOF" > ${LOGS_PATH}/individual_target_creation.log
	echo "CREATE DATABASE \"$anansiblevar_DATABASE_VALUE\" WITH OWNER '$anansiblevar_USER_VALUE';" >> ${LOGS_PATH}/individual_target_creation.log

	#MODIFICO PASSWORD SEGUN VAR
	#echo -e "${BICyan}FIXING ROLE PASSWORD... ${No_Color}"; echo
	echo "ALTER ROLE \"${anansiblevar_USER_VALUE}\" WITH PASSWORD '${anansiblevar_PASSWORD_VALUE}';" >> ${LOGS_PATH}/individual_target_creation.log

	#OBTENGO SETTINGS DEL ROLE
	#echo -e "${BICyan}GETTING ORIGIN ROLE SETTINGS... ${No_Color}"; echo
	
	psql -t --host=$anansiblevar_HOST_VALUE -U $PRODUCTIVE_RDS_USER -d postgres >> ${LOGS_PATH}/individual_target_creation.log <<EOF
SELECT 'ALTER ROLE "$anansiblevar_USER_VALUE" SET '||replace(quote_ident(unnest(setconfig)),'=','"="')||';'
FROM pg_roles r
INNER JOIN pg_db_role_setting rs ON rs.setrole = r.oid
LEFT JOIN pg_database d ON d.oid = rs.setdatabase
WHERE r.rolname = '$anansiblevar_USER_VALUE';
EOF
	#SETEO SETTINGS EN MS
	echo -e "${BIRed}SETTING UP MS SETTINGS: $anansiblevar_USER_VALUE ${No_Color}"; echo
	echo "EOF" >>  ${LOGS_PATH}/individual_target_creation.log
	sed -i 's/^ *//' ${LOGS_PATH}/individual_target_creation.log
	sed -i '/^$/d' ${LOGS_PATH}/individual_target_creation.log
	cat ${LOGS_PATH}/individual_target_creation.log
	echo ""
	cat ${LOGS_PATH}/individual_target_creation.log >> ${LOGS_PATH}/target_creation.log

	#SETEO EL LIMIT EN TARGET BASADO EN ORIGEN
	echo -e "${BIRed}SETTING UP ROLE CONNECTION LIMIT: $anansiblevar_USER_VALUE ${No_Color}"; echo
	echo "psql -t --host=$NEW_RDS_HOST -U $PRODUCTIVE_RDS_USER -d postgres <<EOF" > ${LOGS_PATH}/individual_role_connection_limit.log
	psql -t --host=$anansiblevar_HOST_VALUE -U $PRODUCTIVE_RDS_USER -d postgres >> ${LOGS_PATH}/individual_role_connection_limit.log <<EOF
SELECT 'ALTER ROLE "$anansiblevar_USER_VALUE" WITH CONNECTION LIMIT ' || rolconnlimit || ';' FROM pg_roles WHERE rolname = '$anansiblevar_USER_VALUE';
EOF
	sed -i 's/^ *//' ${LOGS_PATH}/individual_role_connection_limit.log
	sed -i '/^$/d' ${LOGS_PATH}/individual_role_connection_limit.log
	echo "EOF" >>  ${LOGS_PATH}/individual_role_connection_limit.log
	cat ${LOGS_PATH}/individual_role_connection_limit.log
	echo ""
	cat ${LOGS_PATH}/individual_role_connection_limit.log >> ${LOGS_PATH}/role_connection_limit.log

	#LIMIT 0 EN ROL DB ORIGEN
	echo -e "${BIRed}CUTTING ORIGIN ROLE CONNECTION LIMIT: $anansiblevar_USER_VALUE ${No_Color}"; echo
	echo "psql -t --host=$anansiblevar_HOST_VALUE -U $PRODUCTIVE_RDS_USER -d postgres <<EOF" > ${LOGS_PATH}/individual_role_disable_connection_limit.log
	echo "ALTER ROLE \"$anansiblevar_USER_VALUE\" WITH CONNECTION LIMIT 0;" >> ${LOGS_PATH}/individual_role_disable_connection_limit.log
	echo "EOF" >>  ${LOGS_PATH}/individual_role_disable_connection_limit.log
	cat ${LOGS_PATH}/individual_role_disable_connection_limit.log
	echo ""
	cat ${LOGS_PATH}/individual_role_disable_connection_limit.log >> ${LOGS_PATH}/role_disable_connection_limit.log
	
	#MATO CONEXIONES EN ORIGEN
	echo -e "${BIRed}KILLING ACTUAL CONNECTIONS: $anansiblevar_DATABASE_VALUE ${No_Color}"; echo
	echo "psql -t --host=$anansiblevar_HOST_VALUE -U $PRODUCTIVE_RDS_USER -d postgres <<EOF" > ${LOGS_PATH}/individual_killing_connections.log
	echo "SELECT usename, COUNT(pg_terminate_backend(pg_stat_activity.pid)) FROM pg_stat_activity WHERE datname = '$anansiblevar_DATABASE_VALUE';" >> ${LOGS_PATH}/individual_killing_connections.log
	echo "EOF" >> ${LOGS_PATH}/individual_killing_connections.log
	cat ${LOGS_PATH}/individual_killing_connections.log
	echo ""
	cat ${LOGS_PATH}/individual_killing_connections.log >> ${LOGS_PATH}/killing_connections.log
	
	#GENERO DUMP Y RESTORE
	echo -e "${BIRed}GENERATING DUMP SENTENCE: ${No_Color}"; echo
	echo "pg_dump -v --host=\"$anansiblevar_HOST_VALUE\" --username=$PRODUCTIVE_RDS_USER  \"$anansiblevar_DATABASE_VALUE\" | psql --host=\"$NEW_RDS_HOST\" --username=$PRODUCTIVE_RDS_USER \"$anansiblevar_DATABASE_VALUE\"" > ${LOGS_PATH}/individual_dump_sentences.log
	cat ${LOGS_PATH}/individual_dump_sentences.log
	echo ""
	cat ${LOGS_PATH}/individual_dump_sentences.log >> ${LOGS_PATH}/dump_sentences.log
	
	#RENOMBRO DB ORIGEN
	echo -e "${BIRed}RENAMING ORIGIN DATABASE: ${No_Color}"; echo
	echo "psql -t --host=$anansiblevar_HOST_VALUE -U $PRODUCTIVE_RDS_USER -d postgres <<EOF" > ${LOGS_PATH}/individual_alter_database_name.log
        echo "ALTER DATABASE \"$anansiblevar_DATABASE_VALUE\" RENAME TO '"$anansiblevar_DATABASE_VALUE"_delete_me';" >> ${LOGS_PATH}/individual_alter_database_name.log
        echo "EOF" >> ${LOGS_PATH}/individual_alter_database_name.log
	cat ${LOGS_PATH}/individual_alter_database_name.log
	echo ""
	cat ${LOGS_PATH}/individual_alter_database_name.log >> ${LOGS_PATH}/alter_database_name.log
	
	#PYTHON DYNAMO
	echo -e "${BIRed}GENERATING dbacli MODIFICATIONS: ${No_Color}"; echo
	echo "python3 dbacli variables put_variable --name $anansiblevar_HOST --value ${NEW_RDS_HOST} --country ${COUNTRY_CODE,,}" > ${LOGS_PATH}/individual_python_vars.log
	cat ${LOGS_PATH}/individual_python_vars.log
	echo ""
	cat ${LOGS_PATH}/individual_python_vars.log >> ${LOGS_PATH}/python_vars.log

	echo; echo -e "${BIWhite}--------- MIGRATION OVER ${ms^^} FINISHED --------${No_Color}"; echo
    else
	echo -e "${BIRed}ROLE: $anansiblevar_USER_VALUE IS ALREADY IN TARGET -> $anansiblevar_HOST_VALUE ${No_Color}"; echo
    fi
	
done

exit 1
