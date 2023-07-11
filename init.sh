#!/bin/bash
function authority() {
expect << EOF
set timeout -1
spawn ssh-copy-id -i /root/.ssh/id_rsa root@$1
expect { 
    "yes/no" { send "yes\r"; exp_continue }
    "password" { send "root\r" }
}
expect eof
EOF
}

if [ "$(hostname)" = "master-node" ]; then
    #flink
    echo "taskmanager.host: $(hostname)" >> /home/flink/conf/flink-conf.yaml
    
    #dfs
    authority "master-node"
    authority "slave-node"
    hdfs namenode -format
    /home/hadoop/sbin/start-dfs.sh

    #mysql
    # mysql -hmysql-node -uroot -proot -P3306 < mysql_init.sql
    # mysqlbinlog -hmysql-node -uroot -proot --read-from-remote-server binlog.000001 > /tmp/t.binlog

elif [ "$(hostname)" = "slave-node" ]; then
    #flink
    echo "taskmanager.host: $(hostname)" >> /home/flink/conf/flink-conf.yaml

    #yarn
    authority 'master-node'
    authority 'slave-node'
    /home/hadoop/sbin/start-yarn.sh

    #hive
    hadoop fs -mkdir -p /user/hive/warehouse
    hadoop fs -chmod g+w /user/hive/warehouse
    hadoop fs -mkdir /tmp
    hadoop fs -chmod g+w /tmp
    schematool -dbType mysql -initSchema
    nohup hive --service metastore -p 9083 2&> /home/hive/log/hms.log &
    # sleep 1s
    # nohup hive --hiveconf hive.root.logger=DEBUG,console --service metastore -p 9083 2&> /home/hive/log/hms.log &
    # nohup hive --service hiveserver2 2&> /home/hive/log/hs2.log &
    # beeline -u jdbc:hive2://localhost:10000 -n root
    # mysql -u root -p 

    #flink
    nohup start-cluster.sh 2&> /home/flink/log/start-cluster.log &
    # sleep 1s
    nohup sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=slave-node 2&> /home/flink/log/sql-gateway.log &
    # ./sql-client.sh gateway -e localhost:8083
    sleep 1s

elif [ "$(hostname)" = "db-node" ]; then

    #psql
    pg_ctlcluster 12 main start
    su - postgres -c "psql -c \"ALTER USER postgres PASSWORD '123456'\""
    psql -h db-node -p 5432 -U postgres -f psql_init.sql

fi
