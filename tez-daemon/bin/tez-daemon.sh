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


# Runs a yarn command as a daemon.
#
# Environment Variables
#
#   TEZ_DAEMON_HOME - Directory with jars (untar tez)
#   TEZ_DAEMON_BIN_HOME - Directory with binaries
#   TEZ_DAEMON_CONF_DIR Conf dir for tez-daemon-site.xml
#   TEZ_DAEMON_LOG_DIR - defaults to /tmp
#   TEZ_DAEMON_PID_DIR   The pid files are stored. /tmp by default.
#   TEZ_DAEMON_NICENESS The scheduling priority for daemons. Defaults to 0.
##

#set -x

usage="Usage: tez-daemon.sh  (start|stop) "

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# get arguments
startStop=$1
shift


rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ "${TEZ_DAEMON_CONF_DIR}" = "" ] ; then
  echo "TEZ_DAEMON_CONF_DIR must be specified"
  exit 1
fi

if [ -f "${TEZ_DAEMON_CONF_DIR}/tez-daemon-env.sh" ] ; then
  . "${TEZ_DAEMON_CONF_DIR}/tez-daemon-env.sh"
fi

# get log directory
if [ "$TEZ_DAEMON_LOG_DIR" = "" ]; then
  export TEZ_DAEMON_LOG_DIR="/tmp/tezDaemonLogs"
fi

if [ ! -w "$TEZ_DAEMON_LOG_DIR" ] ; then
  mkdir -p "$TEZ_DAEMON_LOG_DIR"
  chown $USER $TEZ_DAEMON_LOG_DIR
fi

if [ "$TEZ_DAEMON_PID_DIR" = "" ]; then
  TEZ_DAEMON_PID_DIR=/tmp
fi

# some variables
TEZ_DAEMON_LOG_BASE=tez-daemon-$USER-$HOSTNAME
export TEZ_DAEMON_LOG_FILE=$TEZ_DAEMON_LOG_BASE.log
if [ ! -n "${TEZ_DAEMON_LOGGER}" ]; then
  echo "TEZ_DAEMON_LOGGER not defined... using defaults"
  TEZ_DAEMON_LOGGER=${LOG_LEVEL_DEFAULT}
fi
logLog=$TEZ_DAEMON_LOG_DIR/$TEZ_DAEMON_LOG_BASE.log
logOut=$TEZ_DAEMON_LOG_DIR/$TEZ_DAEMON_LOG_BASE.out
pid=$TEZ_DAEMON_PID_DIR/tez-daemon-$USER.pid 
#KKK
TEZ_DAEMON_STOP_TIMEOUT=${TEZ_DAEMON_STOP_TIMEOUT:-2}

# Set default scheduling priority
if [ "$TEZ_DAEMON_NICENESS" = "" ]; then
    export TEZ_DAEMON_NICENESS=0
fi

case $startStop in

  (start)

    [ -w "$TEZ_DAEMON_PID_DIR" ] || mkdir -p "$TEZ_DAEMON_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo tezdaemon running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    #rotate_log $logLog
    #rotate_log $logOut
    echo starting tezdaemon, logging to $logLog and $logOut
    nohup nice -n $TEZ_DAEMON_NICENESS "$TEZ_DAEMON_BIN_HOME"/bin/runTezDaemon.sh run  > "$logOut" 2>&1 < /dev/null &
    echo $! > $pid
    ;;
          
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping tezDaemon
        kill $TARGET_PID
        sleep $TEZ_DAEMON_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "tezDaemon did not stop gracefully after $TEZ_DAEMON_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no tezDaemon to stop
      fi
      rm -f $pid
    else
      echo no tezDaemon to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


