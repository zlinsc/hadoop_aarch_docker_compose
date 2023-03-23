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
    #dfs
    authority "master-node"
    authority "slave-node"
    hdfs namenode -format
    /home/hadoop/sbin/start-dfs.sh
elif [ "$(hostname)" = "slave-node" ]; then
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
    sleep 1s
    # nohup hive --hiveconf hive.root.logger=DEBUG,console --service metastore -p 9083 2&> /home/hive/log/hms.log &
    # nohup hive --service hiveserver2 2&> /home/hive/log/hs2.log &
    # beeline -u jdbc:hive2://localhost:10000 -n root
    # mysql -u root -p 
fi
