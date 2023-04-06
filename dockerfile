FROM ubuntu:14.04
LABEL org.opencontainers.image.authors="jeff"

WORKDIR /home

EXPOSE 22
RUN apt-get update && apt-get install -y openssh-server wget expect telnet

# authority
RUN echo "root:root" | chpasswd && \
    echo "root   ALL=(ALL)       ALL" >> /etc/sudoers && \
    ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa -q && \
    sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/g' /etc/ssh/sshd_config

# jdk & hadoop & hive install
ADD jdk-8u361-linux-aarch64.tar.gz .
ADD hadoop-3.3.1-aarch64.tar.gz .
ADD mysql-8.0.31-linux-glibc2.17-aarch64.tar.gz .
ADD apache-hive-3.1.3-bin.tar.gz .
ADD flink-1.17.0-bin-scala_2.12.tgz .
RUN mv jdk* jdk && \
    mv hadoop* hadoop && \
    mv mysql* mysql && \
    mv apache-hive* hive && \
    mv flink* flink

ENV JAVA_HOME /home/jdk
ENV CLASSPATH=$JAVA_HOME/lib:$CLASSPATH
ENV PATH ${JAVA_HOME}/bin:$PATH

ENV HADOOP_HOME /home/hadoop
ENV PATH ${HADOOP_HOME}/bin:$PATH

ENV HIVE_HOME /home/hive
ENV PATH ${HIVE_HOME}/bin:$PATH

ENV FLINK_HOME /home/flink
ENV PATH ${FLINK_HOME}/bin:$PATH

RUN mkdir -p hadoop/tmp && \
    mkdir -p storage/hdfs/name && \
    mkdir -p storage/hdfs/data && \
    mkdir -p hive/log

COPY hadoop-conf/* hadoop/tmp

RUN mv hadoop/etc/hadoop/core-site.xml hadoop/etc/hadoop/core-site.xml.bak && \
    mv hadoop/etc/hadoop/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml.bak && \
    mv hadoop/etc/hadoop/mapred-site.xml hadoop/etc/hadoop/mapred-site.xml.bak && \
    mv hadoop/etc/hadoop/yarn-site.xml hadoop/etc/hadoop/yarn-site.xml.bak && \
    mv hadoop/etc/hadoop/workers hadoop/etc/hadoop/workers.bak && \
    mv flink/bin/taskmanager.sh flink/bin/taskmanager.sh.bak && \
    mv flink/conf/flink-conf.yaml flink/conf/flink-conf.yaml.bak

RUN mv hadoop/tmp/hive-site.xml hive/conf/ && \
    mv hadoop/tmp/taskmanager.sh flink/bin/ && \
    mv hadoop/tmp/flink-conf.yaml flink/conf/ && \
    mv hadoop/tmp/* hadoop/etc/hadoop/

RUN echo '\n\
export JAVA_HOME=/home/jdk\n\
export HDFS_NAMENODE_USER=root\n\
export HDFS_DATANODE_USER=root\n\
export HDFS_SECONDARYNAMENODE_USER=root\n\
export YARN_RESOURCEMANAGER_USER=root\n\
export YARN_NODEMANAGER_USER=root\n'\
>> hadoop/etc/hadoop/hadoop-env.sh

COPY mysql-connector-j-8.0.32.jar hive/lib/

# init cmd
CMD ["/bin/bash", "-c", "service ssh start; /bin/bash"]
