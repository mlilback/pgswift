#!/bin/bash

# all tests should execute "drop owned by current user"

# stop docker container on exit
function finish {
	docker stop rc2-test
}
#trap finish EXIT

docker run --name pgtest -e POSTGRES_PASSWORD="pgtest" -p 5433:5432 --rm -d postgres:9
echo "waiting for db to start"
sleep 10;
docker logs pgtest
docker exec pg-test psql -U postgres -c "create database test;"
docker exec pg-test psql -U postgres -c "create user test superuser password 'secret';"
docker exec pg-test psql -U postgres -c "grant all privileges on database test to test;"

sleep 1;
echo "server ready"

#while true
#do
#	sleep 10
#done

#swift test

