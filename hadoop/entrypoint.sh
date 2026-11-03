#!/bin/bash
set -eu
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_USER_NAME=root
export PATH="${HADOOP_HOME}/bin:${PATH}"
mkdir -p /tmp/hadoop/namenode /tmp/hadoop/datanode /tmp/hadoop-logs
if [ "${1:-}" = "namenode" ]; then
  if [ ! -f /tmp/hadoop/namenode/current/VERSION ]; then
    hdfs namenode -format -force -nonInteractive
  fi
  exec hdfs namenode
fi
exec hdfs datanode
