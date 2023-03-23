#!/bin/zsh
if [ "$1" = "rebuild" ]; then
    docker-compose down
    docker image rm jeff/hadoop 
    docker build -f dockerfile-ubuntu -t jeff/hadoop:latest . 
    docker-compose up -d
fi

docker cp init.sh hadoop-master-node-1:/home
docker cp init.sh hadoop-slave-node-1:/home
docker exec -it hadoop-master-node-1 /home/init.sh 
docker exec -it hadoop-slave-node-1 /home/init.sh 