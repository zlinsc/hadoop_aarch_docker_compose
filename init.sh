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
    echo -e '\n\nexport HADOOP_CLASSPATH=`hadoop classpath`\n' >> /root/.bashrc

    #mysql
    # mysql -hmysql-node -uroot -proot -P3306 < init_mysql.sql
    # mysqlbinlog -hmysql-node -uroot -proot --read-from-remote-server binlog.000001 > /tmp/t.binlog

    #kafka
    zookeeper-server-start.sh -daemon /home/kafka/config/zookeeper.properties
    kafka-server-start.sh -daemon /home/kafka/config/server.properties
    sleep 1s
    kafka-topics.sh --create --zookeeper localhost:2181 --topic cdctest --partitions 4 --replication-factor 1

elif [ "$(hostname)" = "slave-node" ]; then
    #flink
    echo "taskmanager.host: $(hostname)" >> /home/flink/conf/flink-conf.yaml

    #yarn
    authority 'master-node'
    authority 'slave-node'
    /home/hadoop/sbin/start-yarn.sh
    # $HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
    echo -e '\n\nexport HADOOP_CLASSPATH=`hadoop classpath`\n' >> /root/.bashrc

    #hive
    hadoop fs -mkdir -p /user/hive/warehouse
    hadoop fs -chmod g+w /user/hive/warehouse
    hadoop fs -mkdir /tmp
    hadoop fs -chmod g+w /tmp
    schematool -dbType mysql -initSchema
    nohup hive --service metastore -p 9083 &> /home/hive/log/hms.log &
    # nohup hive --hiveconf hive.root.logger=DEBUG,console --service metastore -p 9083 &> /home/hive/log/hms.log &
    sleep 1
    nohup hive --service hiveserver2 &> /home/hive/log/hs2.log &
    # beeline -u jdbc:hive2://slave-node:10000 -n root

    #flink
    hadoop fs -mkdir -p hdfs://master-node:50070/tmp/checkpoints
    # export HADOOP_CLASSPATH=`hadoop classpath`
    # yarn-session.sh --detached -Dyarn.application.name=flinksql -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
    #     -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1
    # sql-client.sh embedded -i /home/init_flink.sql -s yarn-session

    # nohup start-cluster.sh &> /home/flink/log/start-cluster.log &
    # nohup sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=slave-node &> /home/flink/log/sql-gateway.log &
    # ./sql-client.sh gateway -e localhost:8083

    sleep 1s

elif [ "$(hostname)" = "db-node" ]; then

    #psql
    pg_ctlcluster 12 main start
    su - postgres -c "psql -c \"ALTER USER postgres PASSWORD '123456'\""
    psql -h db-node -p 5432 -U postgres -f init_psql.sql

    #mysql
    service mysql start
    mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql mysql
    mv /home/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf
    service mysql restart
    mysql < init_mysql.sql

fi
