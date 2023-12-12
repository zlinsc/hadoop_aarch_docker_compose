FROM ubuntu:18.04
LABEL org.opencontainers.image.authors="jeff"
ENV TZ=Asia/Shanghai

WORKDIR /home

EXPOSE 22
RUN apt update && apt install -y openssh-server wget expect telnet net-tools curl ca-certificates gnupg vim less

# install postgresql
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null && echo "deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list && apt update && apt install -y postgresql-12

# install mysql
RUN apt install -y mysql-server
# RUN wget https://downloads.mysql.com/archives/get/p/23/file/mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN tar -zxvf mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN rm mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN mv mysql* mysql
# RUN mv mysql /usr/local/
# RUN apt install -y libaio1 numactl
# RUN apt install -y mysql-server mysql-client

# copy jar
RUN mkdir -p jars
COPY jars/*.jar jars

# authority
RUN echo "root:root" | chpasswd && \
    echo "root   ALL=(ALL)       ALL" >> /etc/sudoers && \
    ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa -q && \
    # sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/g' /etc/ssh/sshd_config

# jdk & hadoop & hive & flink install
# wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1-aarch64.tar.gz
ADD install/hadoop-3.3.1-aarch64.tar.gz .
# wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
ADD install/apache-hive-3.1.3-bin.tar.gz .
# wget https://dlcdn.apache.org/flink/flink-1.17.0/flink-1.17.0-bin-scala_2.12.tgz
ADD install/flink-1.17.0-bin-scala_2.12.tgz .
# wget https://downloads.mysql.com/archives/get/p/23/file/mysql-8.0.31-linux-glibc2.17-aarch64.tar.gz
# ADD mysql-8.0.31-linux-glibc2.17-aarch64.tar.gz .
# wget https://download.oracle.com/otn/java/jdk/8u361-b09/0ae14417abb444ebb02b9815e2103550/jdk-8u361-linux-aarch64.tar.gz
ADD install/jdk-8u361-linux-aarch64.tar.gz .
ADD install/kafka_2.11-2.0.1.tgz .
# ADD install/spark-3.1.3-bin-hadoop3.2.tgz .
ADD install/spark-3.2.1-bin-hadoop3.2.tgz .

RUN mv jdk* jdk && \
    mv hadoop* hadoop && \
    mv apache-hive* hive && \
    mv flink* flink && \
    mv kafka* kafka && \
    mv spark* spark
    # mv mysql* mysql

ENV JAVA_HOME /home/jdk
ENV CLASSPATH=$JAVA_HOME/lib:$CLASSPATH
ENV PATH ${JAVA_HOME}/bin:$PATH

ENV HADOOP_HOME /home/hadoop
ENV PATH ${HADOOP_HOME}/bin:$PATH

ENV HIVE_HOME /home/hive
ENV PATH ${HIVE_HOME}/bin:$PATH

ENV FLINK_HOME /home/flink
ENV PATH ${FLINK_HOME}/bin:$PATH

ENV KAFKA_HOME /home/kafka
ENV PATH ${KAFKA_HOME}/bin:$PATH

ENV SPARK_HOME /home/spark
ENV PATH ${SPARK_HOME}/bin:$PATH

# ENV MYSQL_HOME /usr/local/mysql
# ENV MYSQL_HOME /home/mysql
# ENV PATH ${MYSQL_HOME}/bin:$PATH

# hadoop & hive & flink initialization
RUN mkdir -p storage/hdfs/name && \
    mkdir -p storage/hdfs/data && \
    mkdir -p hive/log && \
    mkdir -p /var/log/hadoop-yarn

RUN mv hadoop/etc/hadoop/core-site.xml hadoop/etc/hadoop/core-site.xml.bak && \
    mv hadoop/etc/hadoop/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml.bak && \
    mv hadoop/etc/hadoop/mapred-site.xml hadoop/etc/hadoop/mapred-site.xml.bak && \
    mv hadoop/etc/hadoop/yarn-site.xml hadoop/etc/hadoop/yarn-site.xml.bak && \
    mv flink/bin/taskmanager.sh flink/bin/taskmanager.sh.bak && \
    mv flink/conf/flink-conf.yaml flink/conf/flink-conf.yaml.bak && \
    mv kafka/config/server.properties kafka/config/server.properties.bak && \
    mv kafka/bin/kafka-run-class.sh kafka/bin/kafka-run-class.sh.bak

COPY conf/hadoop/* hadoop/etc/hadoop/
COPY conf/hive/hive-site.xml hive/conf/
COPY conf/flink/taskmanager.sh flink/bin/
COPY conf/flink/flink-conf.yaml flink/conf/
COPY conf/flink/masters flink/conf/
COPY conf/flink/workers flink/conf/
COPY conf/kafka/server.properties /home/kafka/config/
COPY conf/kafka/kafka-run-class.sh /home/kafka/bin/
COPY conf/spark/spark-defaults.conf /home/spark/conf/
COPY conf/spark/spark-env.sh /home/spark/conf/

# wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.32.tar.gz
RUN cp jars/mysql-connector-j-8.0.31.jar hive/lib

RUN echo '\n\
export JAVA_HOME=/home/jdk\n\
export HDFS_NAMENODE_USER=root\n\
export HDFS_DATANODE_USER=root\n\
export HDFS_SECONDARYNAMENODE_USER=root\n\
export YARN_RESOURCEMANAGER_USER=root\n\
export YARN_NODEMANAGER_USER=root\n'\
>> hadoop/etc/hadoop/hadoop-env.sh

# postgresql setting
RUN mv /etc/postgresql/12/main/postgresql.conf /etc/postgresql/12/main/postgresql.conf.bak
COPY conf/psql/postgresql.conf /etc/postgresql/12/main/
RUN chown postgres:postgres /etc/postgresql/12/main/postgresql.conf
RUN echo "host    all             all             0.0.0.0/0                 md5" >> /etc/postgresql/12/main/pg_hba.conf

# mysql setting
COPY conf/mysql/mysqld.cnf .
# RUN mkdir -p /usr/local/mysql
# COPY conf/mysql/my.cnf .
# RUN mysqld --defaults-file=/home/my.cnf --initialize
# RUN cp /usr/local/mysql/support-files/mysql.server /etc/init.d/mysql

# db startup script
COPY init_mysql.sql .
COPY init_psql.sql .
COPY init_flink.sql .

# flink cdc jar
WORKDIR /home/jars
RUN mv antlr4-runtime-4.8.jar protobuf-java-3.11.4.jar fastjson2-2.0.39.jar \
    jackson-databind-2.10.5.1.jar jackson-core-2.10.5.jar jackson-annotations-2.10.5.jar \
    connect-api-3.2.0.jar connect-runtime-3.2.0.jar connect-json-3.2.0.jar kafka-clients-3.2.0.jar \
    debezium-core-1.9.7.Final.jar debezium-embedded-1.9.7.Final.jar debezium-api-1.9.7.Final.jar debezium-ddl-parser-1.9.7.Final.jar \
    flink-cdc-base-2.4.0.jar flink-connector-debezium-2.4.0.jar flink-state-processor-api-1.17.0.jar /home/flink/lib/
# -- pgsql cdc
RUN mv debezium-connector-postgres-1.9.7.Final.cut.jar flink-connector-postgres-cdc-2.4.0.jar flink-sql-connector-postgres-cdc-2.4.0.cut.jar postgresql-42.5.1.jar /home/flink/lib/
# -- mysql cdc
RUN mv debezium-connector-mysql-1.9.7.Final.cut.jar flink-connector-mysql-cdc-2.4.0.jar flink-sql-connector-mysql-cdc-2.4.0.jar HikariCP-4.0.3.jar mysql-binlog-connector-java-0.27.2.jar mysql-connector-j-8.0.31.jar /home/flink/lib/
# -- hudi
RUN mv hudi-flink1.17-bundle-mod-0.14.0.jar flink-table-common-1.17.0.jar flink-hadoop-compatibility_2.12-1.17.1.jar avro-1.11.1.jar javalin-4.6.7.jar kotlin-stdlib-1.5.32.jar hive-common-3.1.3.jar hadoop-mapreduce-client-core-3.3.1.jar commons-lang-2.6.jar hive-exec-3.1.3-mod.jar calcite-core-1.16.0.jar libfb303-0.9.3.jar /home/flink/lib/
# -- other tools
RUN mv config-1.4.2.jar flink-connector-kafka-1.17.0.jar /home/flink/lib/

# -- hive init for hudi
RUN mv hudi-hadoop-mr-bundle-0.14.0.jar hudi-hive-sync-bundle-0.14.0.jar /home/hive/lib/
RUN cp /home/flink/lib/hudi-flink1.17-bundle-mod-0.14.0.jar /home/hive/lib/

# spark init for hudi
# RUN mv hudi-spark3.1.3-bundle_2.12-0.14.0.jar /home/spark/jars/
RUN mv hudi-spark3.2.1-bundle_2.12-0.14.0.jar /home/spark/jars/
WORKDIR /home

# flink work jar
# COPY flink_work/target/flink_work-1.1.jar .

# docker startup script
COPY init.sh .
COPY flink_work/demo.sh .

# init cmd
RUN rm /etc/localtime
RUN ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
CMD ["/bin/bash", "-c", "service ssh start; /bin/bash"]
