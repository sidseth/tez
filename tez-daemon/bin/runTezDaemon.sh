#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Environment Variables
#   TEZ_DAEMON_HOME
#   TEZ_DAEMON_USER_CLASSPATH
#   TEZ_DAEMON_HEAPSIZE - MB
#   TEZ_DAEMON_OPTS - additional options
#   TEZ_DAEMON_LOGGER - default is INFO,console
#   TEZ_DAEMON_LOG_DIR - defaults to /tmp
#   TEZ_DAEMON_LOG_FILE - 
#   TEZ_DAEMON_CONF_DIR

function print_usage() {
  echo "Usage: tez-daemon.sh [COMMAND]"
  echo "Commands: "
  echo "  classpath             print classpath"
  echo "  run                   run the daemon"
}

# if no args specified, show usage
if [ $# = 0 ]; then
  print_usage
  exit 1
fi

# get arguments
COMMAND=$1
shift


JAVA=$JAVA_HOME/bin/java
LOG_LEVEL_DEFAULT="INFO,console"
JAVA_OPTS_BASE="-server -Djava.net.preferIPv4Stack=true -XX:+UseNUMA -XX:+UseParallelGC -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps"

# CLASSPATH initially contains $HADOOP_CONF_DIR & $YARN_CONF_DIR
if [ ! -d "$HADOOP_CONF_DIR" ]; then
  echo No HADOOP_CONF_DIR set, or is not a directory. 
  echo Please specify it in the environment.
  exit 1
fi

if [ ! -d "${TEZ_DAEMON_HOME}" ]; then
  echo No TEZ_DAEMON_HOME set, or is not a directory. 
  echo Please specify it in the environment.
  exit 1
fi

if [ ! -d "${TEZ_DAEMON_CONF_DIR}" ]; then
  echo No TEZ_DAEMON_CONF_DIR set, or is not a directory. 
  echo Please specify it in the environment.
  exit 1
fi

if [ ! -n "${TEZ_DAEMON_LOGGER}" ]; then
  echo "TEZ_DAEMON_LOGGER not defined... using defaults"
  TEZ_DAEMON_LOGGER=${LOG_LEVEL_DEFAULT}
fi

CLASSPATH=${TEZ_DAEMON_CONF_DIR}:${TEZ_DAEMON_HOME}/*:${TEZ_DAEMON_HOME}/lib/*:`${HADOOP_PREFIX}/bin/hadoop classpath`:.

if [ -n "TEZ_DAEMON_USER_CLASSPATH" ]; then
  CLASSPATH=${CLASSPATH}:${TEZ_DAEMON_USER_CLASSPATH}
fi

if [ ! -n "${TEZ_DAEMON_LOG_DIR}" ]; then
  echo "TEZ_DAEMON_LOG_DIR not defined. Using default"
  TEZ_DAEMON_LOG_DIR="/tmp/tezDaemonLogs"
fi

if [ "$TEZ_DAEMON_LOGFILE" = "" ]; then
  TEZ_DAEMON_LOG_FILE='tezdaemon.log'
fi

if [ "$TEZ_DAEMON_HEAPSIZE" = "" ]; then
  TEZ_DAEMON_HEAPSIZE=4096
fi

# Figure out classes based on the command

if [ "$COMMAND" = "classpath" ] ; then
  echo $CLASSPATH
  exit
elif [ "$COMMAND" = "run" ] ; then
  CLASS='org.apache.tez.daemon.impl.TezDaemon'
fi

TEZ_DAEMON_OPTS="${TEZ_DAEMON_OPTS} ${JAVA_OPTS_BASE}"
TEZ_DAEMON_OPTS="${TEZ_DAEMON_OPTS} -Dlog4j.configuration=tez-daemon-log4j.properties"
TEZ_DAEMON_OPTS="${TEZ_DAEMON_OPTS} -Dtez.daemon.log.dir=${TEZ_DAEMON_LOG_DIR}"
TEZ_DAEMON_OPTS="${TEZ_DAEMON_OPTS} -Dtez.daemon.log.file=${TEZ_DAEMON_LOG_FILE}"
TEZ_DAEMON_OPTS="${TEZ_DAEMON_OPTS} -Dtez.daemon.root.logger=${TEZ_DAEMON_LOGGER}"

exec "$JAVA" -Dproc_tezdaemon -Xms${TEZ_DAEMON_HEAPSIZE}m -Xmx${TEZ_DAEMON_HEAPSIZE}m ${TEZ_DAEMON_OPTS} -classpath "$CLASSPATH" $CLASS "$@"


