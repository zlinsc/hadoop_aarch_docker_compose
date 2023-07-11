FROM ubuntu:18.04
LABEL org.opencontainers.image.authors="jeff"

WORKDIR /home

EXPOSE 22
RUN apt update && apt install -y openssh-server wget expect telnet curl ca-certificates gnupg vim

# install postgresql
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null && echo "deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list && apt update && apt install -y postgresql-12

# install mysql
# RUN wget https://downloads.mysql.com/archives/get/p/23/file/mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN tar -zxvf mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN rm mysql-8.0.32-linux-glibc2.17-aarch64.tar.gz
# RUN mv mysql* mysql
# RUN mv mysql /usr/local/
# RUN apt install -y libaio1 numactl
# RUN apt install -y mysql-server mysql-client

# authority
RUN echo "root:root" | chpasswd && \
    echo "root   ALL=(ALL)       ALL" >> /etc/sudoers && \
    ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa -q && \
    # sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/g' /etc/ssh/sshd_config

# jdk & hadoop & hive & flink install
ADD jdk-8u361-linux-aarch64.tar.gz .
ADD hadoop-3.3.1-aarch64.tar.gz .
ADD apache-hive-3.1.3-bin.tar.gz .
ADD flink-1.17.0-bin-scala_2.12.tgz .
ADD mysql-8.0.31-linux-glibc2.17-aarch64.tar.gz .
RUN mv jdk* jdk && \
    mv hadoop* hadoop && \
    mv apache-hive* hive && \
    mv flink* flink && \
    mv mysql* mysql

ENV JAVA_HOME /home/jdk
ENV CLASSPATH=$JAVA_HOME/lib:$CLASSPATH
ENV PATH ${JAVA_HOME}/bin:$PATH

ENV HADOOP_HOME /home/hadoop
ENV PATH ${HADOOP_HOME}/bin:$PATH

ENV HIVE_HOME /home/hive
ENV PATH ${HIVE_HOME}/bin:$PATH

ENV FLINK_HOME /home/flink
ENV PATH ${FLINK_HOME}/bin:$PATH

# ENV MYSQL_HOME /usr/local/mysql
ENV MYSQL_HOME /home/mysql
ENV PATH ${MYSQL_HOME}/bin:$PATH

# hadoop & hive & flink initialization
RUN mkdir -p storage/hdfs/name && \
    mkdir -p storage/hdfs/data && \
    mkdir -p hive/log

RUN mv hadoop/etc/hadoop/core-site.xml hadoop/etc/hadoop/core-site.xml.bak && \
    mv hadoop/etc/hadoop/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml.bak && \
    mv hadoop/etc/hadoop/mapred-site.xml hadoop/etc/hadoop/mapred-site.xml.bak && \
    mv hadoop/etc/hadoop/yarn-site.xml hadoop/etc/hadoop/yarn-site.xml.bak && \
    mv flink/bin/taskmanager.sh flink/bin/taskmanager.sh.bak && \
    mv flink/conf/flink-conf.yaml flink/conf/flink-conf.yaml.bak

COPY conf/hadoop/* hadoop/etc/hadoop/
COPY conf/hive/hive-site.xml hive/conf/
COPY conf/flink/taskmanager.sh flink/bin/
COPY conf/flink/flink-conf.yaml flink/conf/
COPY conf/flink/masters flink/conf/
COPY conf/flink/workers flink/conf/

COPY mysql-connector-j-8.0.31.jar hive/lib

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
# RUN mkdir -p /usr/local/mysql
# COPY conf/mysql/my.cnf .
# RUN mysqld --defaults-file=/home/my.cnf --initialize
# RUN cp /usr/local/mysql/support-files/mysql.server /etc/init.d/mysql

# db startup script
COPY mysql_init.sql .
COPY psql_init.sql .

# flink cdc jar
COPY antlr4-runtime-4.8.jar protobuf-java-3.11.4.jar flink/lib/
COPY jackson-databind-2.10.5.1.jar jackson-core-2.10.5.jar jackson-annotations-2.10.5.jar flink/lib/
COPY connect-api-3.2.0.jar connect-runtime-3.2.0.jar connect-json-3.2.0.jar kafka-clients-3.2.0.jar flink/lib/
COPY debezium-core-1.9.7.Final.jar debezium-embedded-1.9.7.Final.jar debezium-api-1.9.7.Final.jar flink/lib/
COPY flink-cdc-base-2.4.0.jar flink-connector-debezium-2.4.0.jar flink/lib/
# flink cdc jar - postgres
COPY postgresql-42.5.1.jar debezium-connector-postgres-1.9.7.Final.jar flink-connector-postgres-cdc-2.4.0.jar flink/lib/
# flink cdc jar - mysql 
COPY mysql-connector-j-8.0.31.jar debezium-connector-mysql-1.9.7.Final.jar flink-connector-mysql-cdc-2.4.0.jar flink/lib/

# docker startup script
COPY init.sh .

# init cmd
CMD ["/bin/bash", "-c", "service ssh start; /bin/bash"]
