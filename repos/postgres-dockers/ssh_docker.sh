######USAGE: bash ssh_docker.sh [9,10,11,12,13]. Use one number to identify the postgres version

LIST=$(docker ps -q)

for value in $LIST; do
	docker cp ~/.pgpass $value:/home/.pgpass
	docker exec -i $value /bin/bash -c "chmod 600 /home/.pgpass"
done

docker exec -it postgres_dockers_pg$1_1 env PGPASSFILE=/home/.pgpass /bin/bash
