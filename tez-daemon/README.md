<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Usage Instructions
==================

### Setup Configuration

   - Create a directory - <CONFDIR> to place configuration files in

   - Place tez-daemon-env.sh, tez-daemon-site.xml and tez-daemon-log4j.properties into this directory. Sample files are available under tez-daemon/src/test/resources or tez-daemon/bin

   - Modify tez-daemon-env.sh to setup at least the following

      - TEZ_DAEMON_HOME - the path to the tez jars

      - TEZ_DAEMON_BIN_HOME - the path to the directory containing the tez-daemon scripts (${TEZ_DAEMON_BIN_HOME}/bin/<scripts>)

      - TEZ_DAEMON_HEAPSIZE - size of the JVM heap in MB

   - Optionally setup LOG_DIRS, LOG_LEVEL, PID_DIRS etc

      - TEZ_DAEMON_LOG_DIR=<PathToLogDir>

      - TEZ_DAEMON_LOGGER="INFO,RFA"

      - TEZ_DAEMON_PID_DIR=<PathToPidDir> | Used to start / stop the service

      - TEZ_DAEMON_USER_CLASSPATH - additional classpath to include while starting the daemon

   - Changes to tez-site.xml

      All configuration parameters set in tez-daemon-site.xml which contain ".am." in their name should also be placed in tez-site.xml (temporary - while this is resolved)


###  Setup the following in the environment before start the TezDaemon process

   - Configuration directory for the tez daemon

    export TEZ_DAEMON_CONF_DIR=<CONFDIR> | created earlier

   - Other required properties

    HADOOP_CONF_DIR - directory for hadoop configuration files

    HADOOP_PREFIX - directory where the hadoop binaries are setup


### Setup tez-daemon-site.xml
An example is included under tez-daemon/src/test/resources. This includes descriptions for fields which need to be changed.

Other properties can be found in TezDaemonConfiguration.java

### Setup tez-daemon-log4j.properties
An example is include under tez-daemon/src/test/resources.


### Running the tez-daemon
Execute tez-daemon/bin/tez-daemon.sh start


### Getting a job to execute within the daemon instead of in separate containers
Setup 'tez.daemon.mode=true' either in tez-site.xml, via a system property for tez-examples, or as part of the configuration when creating an instance of TezClient

Also, set 'tez.am.dag.scheduler.class' to 'org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled' to prevent potential deadlocks.