#!/bin/zsh
mvn clean package
flink run -m localhost:8081 -c NetcatDemo target/flink_work-1.0-SNAPSHOT-jar-with-dependencies.jar