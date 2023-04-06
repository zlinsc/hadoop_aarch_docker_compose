# hadoop_aarch_docker_compose
This repository provides a docker-compose setting to build a runnable hadoop environment, if you use Mac M1 laptop (based on arm chip).

## before start up
Before execute startup.sh shell, we have to download some essential packages using wget.sh.  

## hudi rebuild
As hudi-0.13.0 does not support hive-3 well enough, you will meet the problem that incompatible types cannot be converted (have to switch DateWritable to DateWritableV2). In order to rebuild hudi packages, you may need to install arm version of maven(brew install) and [zulu-jdk](https://www.azul.com/downloads/?package=jdk#zulu) first.
