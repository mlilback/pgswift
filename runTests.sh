#!/bin/bash

# all tests should execute "drop owned by current user"

# stop docker container on exit
function finish {
	docker stop rc2-test
}
#trap finish EXIT

docker run --name pgtest -e POSTGRES_PASSWORD="pgtest" -p 5433:5432 --rm -d postgres:9
echo "waiting for db to start"
sleep 5;
docker logs pgtest
docker exec pgtest psql -U postgres -c "create database test;"
docker exec pgtest psql -U postgres -c "create user test superuser password 'secret';"
docker exec pgtest psql -U postgres -c "grant all privileges on database test to test;"

sleep 1;
echo "server ready"

#while true
#do
#	sleep 5
#done

#swift test

