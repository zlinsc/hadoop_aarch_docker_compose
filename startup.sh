#!/bin/zsh
docker-compose down
docker image rm jeff/hadoop 
docker build -f dockerfile -t jeff/hadoop:latest . 
docker-compose up -d # docker-compose.yml

dir_name=hadoop_aarch_docker_compose
docker exec -it $dir_name-master-node-1 /home/init.sh 
docker exec -it $dir_name-slave-node-1 /home/init.sh 
docker exec -it $dir_name-db-node-1 /home/init.sh 
