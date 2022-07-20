#!/bin/bash

#set -x
. ~/.colors

_usage() { echo "Usage: upgrade_instance.sh [-c <A|B|CCC|CC|E|x1|UY|C|xx1|DEV|TOOL|xx1>] [-i <ACTUAL_INSTANCE_ALIAS>] [-e <EXECUTE_MODIFICATIONS>] [-t <MINOR|MAJOR> ]" 1>&2; exit 1; }

####VALIDACIONES DE PARAMETROS INGRESADOS
while getopts "c:i:e:" o; do
    case "${o}" in
        c)
            c=${OPTARG^^}
        [ "${c}" == "a" -o "${c}" == "b" -o "${c}" == "c" -o  "${c}" == "cc" -o "${c}" == "e" -o "${c}" == "p" -o "${c}" == "u" -o "${c}" == "c" -o "${c}" == "m" -o "${c}" == "DEV" -o "${c}" == "TOOL" -o "${c}" == "xx1" ] || _usage
            ;;
        i)
            i=${OPTARG}
            ;;
	e)
            e=${OPTARG}
            ;;
        *)
            _usage
            ;;
    esac
done

####IMPIDO CONTINUAR SI NO INGRESAN COUNTRY NI INSTANCE ALIAS
if [[ -z "${c}" || -z "${i}" ]]; then
    _usage
fi

####ASIGNACION DE VARIABLES Y CREACION DE FILES DE LOGGING + PERMISOS
mkdir -p ~/postgresql/common/customer_queries/MAJOR_UPGRADE
chmod 755 -R ~/postgresql/common/customer_queries/MAJOR_UPGRADE
ROOT_PATH=~/postgresql/common/customer_queries/MAJOR_UPGRADE
COUNTRY_CODE=${c}
mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}
GS_RDS_NAME=${i}
mkdir -p ${ROOT_PATH}/${COUNTRY_CODE}/${GS_RDS_NAME}
LOGS_PATH=${ROOT_PATH}/${COUNTRY_CODE}/${GS_RDS_NAME}/LOGS
mkdir -p ${LOGS_PATH}
chmod 755 -R ${LOGS_PATH}
EXECUTE_IMPLEMENTATION=${e}
PRODUCTIVE_RDS_USER=dba_test_service
POSSIBLE_VERSIONS=${LOGS_PATH}/possible_versions.$(date +%Y%m%d%H%M%S).log
ACTUAL_ENGINE=${LOGS_PATH}/actual_engine_rds_instance.$(date +%Y%m%d%H%M%S).log
DATABASES=${LOGS_PATH}/actual_databases.$(date +%Y%m%d%H%M%S).log
DATA_TYPES_CHECK=${LOGS_PATH}/data_types_check.$(date +%Y%m%d%H%M%S).log
REPLICATION_SLOTS=${LOGS_PATH}/replication_slots_check.$(date +%Y%m%d%H%M%S).log
DROPPING_REPLICATION_SLOTS=${LOGS_PATH}/dropping_replication_slots_check.$(date +%Y%m%d%H%M%S).sql
RECREATING_REPLICATION_SLOTS=${LOGS_PATH}/recreating_replication_slots_check.$(date +%Y%m%d%H%M%S).sh
RECREATE_EXTENSIONS=${LOGS_PATH}/recreating_extensions.$(date +%Y%m%d%H%M%S).sh
DROPPING_EXTENSIONS=${LOGS_PATH}/dropping_extensions.$(date +%Y%m%d%H%M%S).sh
TEMP_MANAGE_EXTENSIONS=${LOGS_PATH}/temp_manage_extensions.$(date +%Y%m%d%H%M%S).sql
GENERAL_LOG=${LOGS_PATH}/general_log.$(date +%Y%m%d%H%M%S).log
TEMP_MANAGE_ROLE_CONNECTIONS=${LOGS_PATH}/prepare_manage_role_connections.$(date +%Y%m%d%H%M%S).sql
TEMP_DISABLE_ROLE_CONNECTIONS=${LOGS_PATH}/prepare_disable_role_connections.$(date +%Y%m%d%H%M%S).sql
TEMP_RESTABLISH_ROLE_CONNECTIONS=${LOGS_PATH}/prepare_restablish_role_connections.$(date +%Y%m%d%H%M%S).sql
REPLICA_INSTANCES=${LOGS_PATH}/replica_instances.$(date +%Y%m%d%H%M%S).log

####DECLARO LA EXISTENCIA DE LOS FILES
touch ${POSSIBLE_VERSIONS}
touch ${ACTUAL_ENGINE}
touch ${DATABASES}
touch ${DATA_TYPES_CHECK}
touch ${REPLICATION_SLOTS}
touch ${DROPPING_REPLICATION_SLOTS}
touch ${RECREATING_REPLICATION_SLOTS}
touch ${RECREATE_EXTENSIONS}
touch ${TEMP_MANAGE_EXTENSIONS}
touch ${DROPPING_EXTENSIONS}
touch ${GENERAL_LOG}
touch ${TEMP_MANAGE_ROLE_CONNECTIONS}
touch ${TEMP_DISABLE_ROLE_CONNECTIONS}
touch ${TEMP_RESTABLISH_ROLE_CONNECTIONS}
touch ${REPLICA_INSTANCES}

####CHEQUEO DE VERSION ACTUAL
echo; echo -e "${BICyan}CHECKING POSSIBILITY TO PERFORM A MAJOR VERSION UPGRADE${No_Color}"; echo

#OBTENCION DEL AWS REGION A PARTIR DEL COUNTRY
PROFILE=$(grep ${COUNTRY_CODE} ~/.ec2_profiles |  cut -d: -f2)

#BUSCO LA VERSION DEL ENGINE EN DBATOOLS (el script sync_metadata_rds_postgresql... lo genera)
awspsql.sh -c TOOL -i DBATools -u ${PRODUCTIVE_RDS_USER} -d dba_tools -a -v values="${GS_RDS_NAME},${COUNTRY_CODE}" -f ${ROOT_PATH}/get_engine_version.sql > ${ACTUAL_ENGINE}
sed -i '/^$/d' ${ACTUAL_ENGINE}
#cat ${ACTUAL_ENGINE}

#SEPARO EN 2 VARIABLES, NOMBRE REAL DE LA INSTANCIA y VERSION DEL ENGINE
RDS_INSTANCE_NAME=$(cut -d\| -f1 ${ACTUAL_ENGINE})
RDS_INSTANCE_ENGINE_VERSION=$(cut -d\| -f2 ${ACTUAL_ENGINE})

#SI LA VERSION ACTUAL ES >=12 FINALIZO EL SCRIPT
if [[ $(echo ${RDS_INSTANCE_ENGINE_VERSION} | grep -e '12\.' | wc -c) -gt 0 ]]; then
	echo -e "${BIGreen}Instance in ${COUNTRY_CODE}, with alias '${GS_RDS_NAME}', identifier '${RDS_INSTANCE_NAME}' is in version PostgreSQL ${RDS_INSTANCE_ENGINE_VERSION} ${No_Color}"; echo
	echo -e "${BIBlue}Nothing to do. Bye!${No_Color}"; echo
	exit 1
fi
 
#FINALIZO SI NO ENCONTRE LA INSTANCIA A PARTIR DE SU ALIAS
if [[ "$(cat ${ACTUAL_ENGINE} | wc -l)" -eq 0 ]]; then
	echo -e "${BIRed}Instance with alias '${GS_RDS_NAME}' not found in RDS dba_tools -> rds_postgresql_endpoints ${No_Color}"; echo
	exit 1
else
	#LE PIDO A AWS LAS POSIBLES VERSIONES A UPGRADEAR. GUARDO EN VARIABLE
	aws rds describe-db-engine-versions --engine postgres --engine-version ${RDS_INSTANCE_ENGINE_VERSION} | grep -A 200 "ValidUpgradeTarget"|grep "EngineVersion"|  sed -e 's/"//g' |sed -e 's/EngineVersion: /PostgreSQL /g' > ${POSSIBLE_VERSIONS}
fi

if [[ $(cat ${POSSIBLE_VERSIONS} | grep -e '12\.' | wc -l) -eq 0 ]]; then
	#SI LA VERSION >=12 NO APARECE, NO PUEDO HACER EL UPGRADE. FINALIZO MOSTRANDO LAS VERSIONES DISPONIBLES
	echo -e "${BIRed}Instance cannot be upgraded to PostgreSQL 12 versions.${No_Color}"; echo
	echo -e "${BICyan}These are available ${No_Color}"; echo
	cat ${POSSIBLE_VERSIONS} | sed 's/,//g' | tr  -d '[:blank:]'; echo
	#PIDO QUE EL ADMIN HAGA UN MINOR VERSION UPGRADE INICIALMENTE
	echo -e "${BIRed}Do first a Minor Version Upgrade ${No_Color}"; echo
	exit 1
else
	#MUESTRO QUE LA VERSION 12 ESTA DISPONIBLE
	echo -e "${BIGreen}Major version upgrade to PostgreSQL 12 available!${No_Color}"; echo
	cat ${POSSIBLE_VERSIONS} | grep -e '12\.' | sed 's/,//g' | tr  -d '[:blank:]'; echo
	NEWEST_VERSION=$(cat ${POSSIBLE_VERSIONS} | grep '12\.' | sed 's/,//g' | tr  -d '[:blank:]' | sed 's/PostgreSQL//g' | tail -1)
	#echo "$NEWEST_VERSION"
fi

####BUSCO TODAS LAS DATABASES EXISTENTES DE LOS MS (QUITO LAS ADMINISTRATIVAS)
awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${ROOT_PATH}/non_admin_databases.sql > ${DATABASES}

#CONTINUO SI EXISTEN DATABASES EN LA INSTANCIA.
if [[ $(cat ${DATABASES} | wc -l) -gt 0 ]]; then
	
	#IMPRIMO HORA DE COMIENZO DEL SCRIPT
	echo "START TIME: $(date '+%Y-%m-%d %H:%M:%S')"; echo
	
	#BUSCO SI EXISTEN COLUMNAS CON DATOS UNKNOWN (aws no permite un upgrade en caso de existencia)
	echo -e "${BICyan}CHECKING UNKNOWN DATA TYPES IN EACH DATABASE... (this may take a while) ${No_Color}"; echo
	
	#RECORRO CADA DB EN BUSQUEDA DE UNKNOWN DATA TYPES
	cat ${DATABASES} | while read -r DATABASE_NAME; do
	#GUARDO EL RESULTADO EN UN FILE
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d $DATABASE_NAME -a -f ${ROOT_PATH}/check_data_types.sql > ${DATA_TYPES_CHECK}
	#APROVECHO LA BUSQUEDA SOBRE CADA DATABASE PARA BUSCAR EXTENSIONES E IR GUARDANDOLAS
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d $DATABASE_NAME -a -f ${ROOT_PATH}/manage_existing_extensions.sql >> ${TEMP_MANAGE_EXTENSIONS}
	
	#SI EXISTEN UNKNOWN DATA TYPES FINALIZO EL SCRIPT
	if [[ $(cat ${DATA_TYPES_CHECK} | wc -l) -gt 0 ]]; then
		echo -e "${BIRed}There are unknown data types in database: '$DATABASE_NAME' that will not allow the upgrade.${No_Color}"; echo
		cat ${DATA_TYPES_CHECK}
		exit 1
	fi
	done
	
	#AGREGO LAS EXTENSIONES EXISTENTES EN DB postgres (NO ESTABA INCLUIDA EN EL COMPILADO DE DATABASES ANTERIOR)
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${ROOT_PATH}/manage_existing_extensions.sql >> ${TEMP_MANAGE_EXTENSIONS}
	
	#MENSAJE DE SUCCESS EN LA INEXISTENCIA DE DATOS DESCONOCIDOS
	echo -e "${BIGreen}There is no unknown data types in any database. We can continue with the upgrade ${No_Color}"; echo
fi

####BUSQUEDA DE SLOTS DE REPLICACION ACTIVOS
echo -e "${BICyan}CHECKING THAT REPLICATION SLOTS ARE EMPTY... (this may take a while) ${No_Color}"; echo

#LOS BUSCO DIRECTAMENTE SOBRE DB postgres YA QUE LAS ESTADISTICAS LO CENTRALIZAN ALLI
awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${ROOT_PATH}/check_replication_slots.sql > ${REPLICATION_SLOTS}

#SI EXISTEN SLOTS DE REPLICACION LO INFORMO (SI ESTAS EN MODO EJECUCION EL SCRIPT SE ENCARGARA DE ELIMINARLOS)
if [[ $(cat ${REPLICATION_SLOTS} | wc -l) -gt 0 ]]; then
        echo -e "${BIRed}There are replication slots that need to be droppped ${No_Color}"; echo
        cat ${REPLICATION_SLOTS}; echo
	
	#RECORRO LOS SLOTS ENCONTRADOS PARA DISCRIMINAR NOMBRE, TIPO, DATABASE, PLUGIN
	cut -d\| -f1,2,3,5 ${REPLICATION_SLOTS} | while IFS=\| read -r SLOT_NAME SLOT_PLUGIN SLOT_TYPE SLOT_DATABASE; do
	#GUARDO LA SENTENCIA A EJECUTAR PARA EL DROP
	echo "SELECT pg_drop_replication_slot('$SLOT_NAME');" >> ${DROPPING_REPLICATION_SLOTS}
	#GUARDO LA SENTENCIA A EJECUTAR PAR EL RECREATE DISCRIMINANDO LA DB A CONECTARSE
	echo "awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d $SLOT_DATABASE <<EOF
SELECT pg_create_logical_replication_slot('$SLOT_NAME', '$SLOT_PLUGIN');
EOF" >> ${RECREATING_REPLICATION_SLOTS}
	
	done
	
	#SI NO ESTOY EN MODO EJECUCION MUESTRO EN PANTALLA LAS SENTENCIAS DE DROP Y RECREATE
	if [[ -z ${e} ]]; then
		cat ${DROPPING_REPLICATION_SLOTS}
		echo
		cat ${RECREATING_REPLICATION_SLOTS}
		
		#AVISO QUE HAY REPLICATION SLOTS QUE SERIAN ELIMINADOS
		echo; echo -e "${BIRed}Cannot continue upgrade until there are no replication slots ${No_Color}"; echo
	#SI ESTOY EN MODO EJECUCION ALMACENO LAS SENTENCIAS EN EL LOG GENERAL
	else
		cat ${DROPPING_REPLICATION_SLOTS} 2>&1 >> ${GENERAL_LOG}
		echo "-------------------------" 2>&1 >> ${GENERAL_LOG}
		cat ${RECREATING_REPLICATION_SLOTS} 2>&1 >> ${GENERAL_LOG}
	fi	
else
	#MUESTRO MENSAJE FELIZ DE QUE NO EXISTEN SLOTS
        echo -e "${BIGreen}No replication slots. Lets continue ${No_Color}"; echo
fi

#RECORDATORIO PARA EL DBA CUANDO EJECUTA EN AMBOS MODOS. BUENA OPORTUNIDAD PARA FRENAR EL SCRIPT EN CASO DE QUE EXISTAN. TODO: IMPLEMENTAR VALIDACION
echo -e "${BICyan}---- REMEMBER TO STOP ANY DMS TASK AND DROP READ REPLICAS ----${No_Color}"; echo

#SEPARO LAS SENTENCIAS DE DROP/CREATE DE EXTENSIONES EN DOS FILES PARA CADA TIPO
cat ${TEMP_MANAGE_EXTENSIONS} | while IFS=\| read -r EXTENSION_DATABASE EXTENSION_CREATE EXTENSION_DROP; do
echo "awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d $EXTENSION_DATABASE <<EOF
$EXTENSION_CREATE
EOF" >> ${RECREATE_EXTENSIONS}
echo "awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d $EXTENSION_DATABASE <<EOF
$EXTENSION_DROP
EOF" >> ${DROPPING_EXTENSIONS}

done

if [[ -z ${e} ]]; then
   	cat ${DROPPING_EXTENSIONS}
	cat ${RECREATE_EXTENSIONS}
else
	#GUARDO EN LOG GENERAL
	cat ${DROPPING_EXTENSIONS} 2>&1 >> ${GENERAL_LOG}
        cat ${RECREATE_EXTENSIONS} 2>&1 >> ${GENERAL_LOG}
fi

#CHEQUEO EL STATUS DE LA INSTANCIA. SI NO ESTA AVAILABLE NO ENTRA EN EL IF POSTERIOR
echo; echo -e "${BICyan}CHECKING INSTANCE STATUS... (this may take a while) ${No_Color}"; echo

#GUARDO EL INSTANCE STATUS
INSTANCE_STATUS=$(aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .DBInstanceStatus' | head -1 | sed 's/\"//g')

echo -e "${BIBlue}Status: $INSTANCE_STATUS ${No_Color}"; echo

#CHEQUEO LA EXISTENCIA DE INSTANCIAS DE REPLICACION
aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .ReadReplicaDBInstanceIdentifiers' | grep -v -e '\[' | grep -v -e '\]' | sed 's/\"//g' | sed 's/,//g' | tr  -d '[:blank:]' > ${REPLICA_INSTANCES}
#GUARDO LAS REPLICAS EN LOG GENERAL
cat ${REPLICA_INSTANCES} 2>&1 >> ${GENERAL_LOG}
#SI HAY INSTANCIAS DE REPLICACION TERMINO EL SCRIPT.
if [[ $(cat ${REPLICA_INSTANCES} | wc -l) -gt 0 ]]; then
        echo -e "${BIRed}There are replica instances that need to be droppped ${No_Color}"; echo
	#MUESTRO LAS REPLICAS A ELIMINAR
        cat ${REPLICA_INSTANCES}; echo
	exit 1
fi

#SI LA INSTANCIA ESTA HABILITADA Y ESTOY EN MODO EJECUCION COMIENZO EL PROCESO
if [[ "${INSTANCE_STATUS}" == 'available' ]] && [[ ${EXECUTE_IMPLEMENTATION} == "EXECUTE" ]]; then
	#MUESTRO LA RUTA DEL GENERAL LOG A REVISAR MIENTRAS SE EJECUTA EL PROCESO COMPLETO
	echo "GENERAL_LOG ${GENERAL_LOG}"; echo
	echo -e "${BICyan}PERFORMING A SNAPSHOT... (this may take a while) ${No_Color}"; echo
	#EJECUTO UN SNAPSHOT CON EL SUFFIX master-12-fecha
	aws rds create-db-snapshot --profile "${PROFILE}" --db-instance-identifier ${RDS_INSTANCE_NAME} --db-snapshot-identifier "snapshot-${RDS_INSTANCE_NAME}-prior-upgrade-$(date '+%Y-%m-%d')"
	echo; echo -e "${BICyan}CREATING PARAMETER GROUP... (this may take a while) ${No_Color}"; echo
	#CREO EL PARAMETER GROUP COMO COPIA DEL DBA DEFAULT. TODO: AGREGAR 4 PARAMETROS DISTINTOS, GRANDES, NORMALES, AUDIT, NOAUDIT
	aws rds copy-db-parameter-group --profile "${PROFILE}" --source-db-parameter-group-identifier "dba-default-postgres12" --target-db-parameter-group-identifier "${RDS_INSTANCE_NAME,,}-master-pg12" --target-db-parameter-group-description "RDS ${GS_RDS_NAME} Parameter Group Postgres 12 Master"	
	echo; echo -e "${BICyan}DROPPING ALL EXTENSIONS... (this may take a while) ${No_Color}"; echo
        #EJECUTO EL SCRIPT DE DROP DE EXTENSIONES (VA POR CADA DATABASE)
	bash ${DROPPING_EXTENSIONS} 2>&1 >> ${GENERAL_LOG} 
	echo; echo -e  "${BICyan}DROPPING REPLICATION SLOTS... ${No_Color}"; echo;
	#EJECUTO EL SCRIPT DE DROP DE SLOTS DE REPLICACION (VA SOBRE db postgres)
        awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${DROPPING_REPLICATION_SLOTS} 2>&1 >> ${GENERAL_LOG}
	#REMUEVO LAS GLOBAL VIEWS
	echo; echo -e "${BICyan}REMOVING GLOBAL VIEWS... (this may take a while) ${No_Color}"; echo
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ~/postgresql/common/drop_pg_fix_global_views_pg11.sql 2>&1 >> ${GENERAL_LOG}
	echo; echo -e "${BICyan}DISABLING ROLE CONNECTIONS... (this may take a while) ${No_Color}"; echo
	#CREO UN FILE CON LAS SENTENCIAS DE QUITE DE CONNLIMIT Y RESTAURACION
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${ROOT_PATH}/prepare_manage_role_connections.sql > ${TEMP_MANAGE_ROLE_CONNECTIONS}
	cat ${TEMP_MANAGE_ROLE_CONNECTIONS} | cut -d\| -f1 > ${TEMP_DISABLE_ROLE_CONNECTIONS}
	cat ${TEMP_MANAGE_ROLE_CONNECTIONS} | cut -d\| -f2 > ${TEMP_RESTABLISH_ROLE_CONNECTIONS}
	#GUARDO EN LOG GENERAL DICHAS SENTENCIAS
	cat ${TEMP_DISABLE_ROLE_CONNECTIONS} 2>&1 >> ${GENERAL_LOG}
	cat ${TEMP_RESTABLISH_ROLE_CONNECTIONS} 2>&1 >> ${GENERAL_LOG}
	#PONGO LOS LIMITS DE LOS ROLES EN 0 (UNICAMENTE LOS MS, NO ASI LOS ROLES ADMIN)
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${TEMP_DISABLE_ROLE_CONNECTIONS} 2>&1 >> ${GENERAL_LOG}
 	
	###COMIENZA EL UPGRADE
	echo; echo -e "${BIBlue}--------------------APPLYING UPGRADE---------------------${No_Color}"; echo; echo
	#APLICO EL UPGRADE VIA AWS. USO EL PROFILE, EL NOMBRE DEL RDS Y EL PARAMETER GROUP CREADO.
	aws rds modify-db-instance --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" --engine-version $NEWEST_VERSION --db-parameter-group-name "${RDS_INSTANCE_NAME,,}-master-pg12" --allow-major-version-upgrade --apply-immediately
	#ESPERO 30 SEGUNDOS PARA QUE AWS CAMBIE EL ESTADO DE LA INSTANCIA A UPGRADING/BACKUP-UP(POR EL SNAPSHOT)
	sleep 30
	#MUESTRO EL ESTADO ACTUAL
	INSTANCE_STATUS=$(aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .DBInstanceStatus' | head -1 | sed 's/\"//g')
	
	echo; echo -e "${BICyan}Waiting for DBInstance to return to available State... ${No_Color}"; echo
	#LOOP ESPERANDO QUE EL ESTADO DE LA INSTANCIA VUELVA A AVAILABLE
	while [ $INSTANCE_STATUS != 'available' ]
	do
		sleep 30
		INSTANCE_STATUS=$(aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .DBInstanceStatus' | head -1 | sed 's/\"//g')
		if [[ $INSTANCE_STATUS  == 'available' ]]; then
			#CORTO EL LOOP EN CASO DE HABER VUELVO AL ESTADO DISPONIBLE
			break
		fi
		#MUESTRO EL ESTADO DE LA INSTANCIA CADA 30 SEGUNDOS
		echo; echo -e "${BIBlue}Status: $INSTANCE_STATUS ${No_Color}"; echo 
	done
	#MUESTRO QUE HAYA QUEDADO AVAILABLE. TODO: VERIFICAR VERSION DEL ENGINE, SI NO SUBIO A 12 APLICAR LAS GLOBAL VIEWS ANTIGUAS
	echo -e "${BIGreen}Status: $INSTANCE_STATUS ${No_Color}"; echo
	echo -e "${BICyan}ENABLING ROLE CONNECTIONS... (this may take a while) ${No_Color}"; echo
	#HABILITO EL LIMIT CONNECTION DE LOS ROLES. GUARDO EN LOG GENERAL EL RESULTADO
	awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ${TEMP_RESTABLISH_ROLE_CONNECTIONS} 2>&1 >> ${GENERAL_LOG}
	echo; echo -e  "${BICyan}RECREATING EXTENSIONS... ${No_Color}"; echo
	#RECREO LAS EXTENSIONES POR CADA DATABASE. GUARDO EN LOG GENERAL EL RESULTADO
	bash ${RECREATE_EXTENSIONS} 2>&1 >> ${GENERAL_LOG}
	echo; echo -e "${BICyan}RECREATING GLOBAL VIEWS... (this may take a while) ${No_Color}"; echo
	#CHEQUEO LA VERSION DE ENGINE QUE QUEDO
	INSTANCE_FINAL_VERSION=$(aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .EngineVersion' | head -1 | sed 's/\"//g')
	#LOOP EN CASO DE QUE AWS FALLE POR THROTLLING DE MANERA DE ASEGURARSE DE TENER UN RESULTADO
	while [ $INSTANCE_FINAL_VERSION != *"9"* && $INSTANCE_FINAL_VERSION != *"12"* ]
        do
		#ESPERO 1 SEGUNDO POR CONSULTA
                sleep 1
		#CHEQUEO EL VALOR DEL ENGINE
		INSTANCE_FINAL_VERSION=$(aws rds describe-db-instances --profile "${PROFILE}" --db-instance-identifier "${RDS_INSTANCE_NAME}" | jq '.DBInstances | .[] | .EngineVersion' | head -1 | sed 's/\"//g')
		#VERIFICO QUE ESTE EN VERSION 9 O 12
		if [[ $INSTANCE_FINAL_VERSION == *"9."* ]] || [[ $INSTANCE_FINAL_VERSION == *"12."* ]]; then
                        #CORTO EL LOOP SI OBTENGO DATOS DE VERSION
                        break
                fi
                #MUESTRO EL ESTADO DE LA INSTANCIA CADA 30 SEGUNDOS
                echo; echo -e "${BIBlue}Status: $INSTANCE_FINAL_VERSION ${No_Color}"; echo
        done
	if [[ $INSTANCE_FINAL_VERSION == *"9."* ]]; then
		#SI LA VERSION ES 9, APLICO LAS GLOBAL VIEWS ANTIGUAS. GUARDO EN LOG GENERAL EL RESULTADO
		awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ~/postgresql/common/pg_fix_global_views.sql 2>&1 >> ${GENERAL_LOG}
	else
		#SI LA VERSION ES 12, APLICO LAS GLOBAL VIEWS ACTUALIZADAS. GUARDO EN LOG GENERAL EL RESULTADO
		awspsql.sh -c ${COUNTRY_CODE} -i ${GS_RDS_NAME} -u dba_test_service -d postgres -a -f ~/postgresql/common/pg_fix_global_views_pg11.sql 2>&1 >> ${GENERAL_LOG}
	fi
	echo; echo -e "${BICyan}RECREATING REPLICATION SLOTS... ${No_Color}"; echo
	#RECREO LOS SLOTS DE REPLIACION EN CADA DATABASE
        bash ${RECREATING_REPLICATION_SLOTS} 2>&1 >> ${GENERAL_LOG}
fi

echo; echo "-----------------ELIMINANDO FILES TEMPORALES CREADOS --------------------"; echo

#ELIMINO LOS FILES CREADOS A EXCEPCION DEL GENERAL LOG
rm -f ${REPLICATION_SLOTS}
rm -f ${DROPPING_REPLICATION_SLOTS}
rm -f ${RECREATING_REPLICATION_SLOTS}
rm -f ${DATA_TYPES_CHECK}
rm -f ${DATABASES}
rm -f ${POSSIBLE_VERSIONS}
rm -f ${ACTUAL_ENGINE}
rm -f ${TEMP_MANAGE_EXTENSIONS}
rm -f ${RECREATE_EXTENSIONS}
rm -f ${DROPPING_EXTENSIONS}
rm -f ${TEMP_MANAGE_ROLE_CONNECTIONS}
rm -f ${TEMP_DISABLE_ROLE_CONNECTIONS}
rm -f ${TEMP_RESTABLISH_ROLE_CONNECTIONS}
rm -f ${REPLICA_INSTANCES}

echo "END TIME: $(date '+%Y-%m-%d %H:%M:%S')"; echo

echo; echo -e "${BIBlue}-----------------------FINISH------------------------${No_Color}"; echo

#MUESTRO EL VALOR FINAL DEL ENGINE
echo "ACTUAL VERSION: $INSTANCE_FINAL_VERSION"

if [[ ${EXECUTE_IMPLEMENTATION} == "EXECUTE" ]] && [[ "${INSTANCE_FINAL_VERSION}" == *"12"* ]]; then
	#DOY EL ESTADO FINAL DE LA MIGRACION. SI LLEGO A 12 FUE EXITOSA. TODO: IMPLEMENTAR LA POSIBILIDAD DE HACER MINOR VERSION UPGRADES
        echo; echo -e "${BIGreen}New instance version: $INSTANCE_FINAL_VERSION. UPGRADE SUCCESSFUL ${No_Color}"; echo
elif [[ ${EXECUTE_IMPLEMENTATION} == "EXECUTE" ]] && [[ "${INSTANCE_FINAL_VERSION}" != *"12"* ]]; then
	#MUESTRO QUE EL UPGRADE NO FUE EXITOSO
        echo; echo -e "${BIRed}New instance version: $INSTANCE_FINAL_VERSION. UPGRADE FAILED. Search in rds aws upgrade logs. ${No_Color}"; echo
fi

exit 1
