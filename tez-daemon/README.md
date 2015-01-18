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

###  Setup the following in the environment before start the TezDaemon process

   -  Heapsize for the running daemon
    export TEZ_DAEMON_HEAPSIZE=5000

   - Logging properties
    export TEZ_DAEMON_LOGGER="INFO,RFA"
    export TEZ_DAEMON_LOG_DIR=<PathToLogDir>
    
   - Other required properties
    HADOOP_CONF_DIR - directory for hadoop configuration files
    HADOOP_PREFIX - directory where the hadoop binaries are setup
    TEZ_PREFIX - directory where the tez binaries are present
    
   - Optional properties
    TEZ_DAEMON_USER_CLASSPATH - additional classpath to include while starting the daemon (not tested)
    
### Setup tez-daemon-site.xml
An example is included under tez-daemon/src/test/resources. This includes descriptions for fields which need to be changed.
The file should be placed in the classpath of the running daemon (Either HADOOP_CONF_DIR or the tez binary dir)

Other properties can be found in TezDaemonConfiguration.java

### Setup tez-daemon-log4j.properties
An example is include under tez-daemon/src/test/resources. This should


### Running the tez-daemon
Execute tez-daemon/bin/tez-daemon.sh run


### Submitting a job
Setup 'tez.daemon.mode=true' either in tez-site.xml, via a system property for tez-examples, or as part of the configuration when creating an instance of TezClient