/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.daemon;

import org.apache.hadoop.conf.Configuration;

public class TezDaemonConfiguration extends Configuration {

  public TezDaemonConfiguration(Configuration conf) {
    super(conf);
    addResource(TEZ_DAEMON_SITE);
  }

  public TezDaemonConfiguration() {
    super(false);
    addResource(TEZ_DAEMON_SITE);
  }


  private static final String TEZ_DAEMON_PREFIX = "tez.daemon.";
  private static final String TEZ_DAEMON_AM_PREFIX = TEZ_DAEMON_PREFIX + "am.";
  private static final String TEZ_DAEMON_SITE = "tez-daemon-site.xml";



  public static final String TEZ_DAEMON_HOSTNAME = TEZ_DAEMON_PREFIX + "hostname";

  public static final String TEZ_DAEMON_RPC_PORT = TEZ_DAEMON_PREFIX + "rpc.port";
  public static final int TEZ_DAEMON_RPC_PORT_DEFAULT = 15001;

  public static final String TEZ_DAEMON_RPC_NUM_HANDLERS = TEZ_DAEMON_PREFIX + "rpc.num.handlers";
  public static final int TEZ_DAEMON_RPC_NUM_HANDLERS_DEFAULT = 5;

  public static final String TEZ_DAEMON_WORK_DIRS = TEZ_DAEMON_PREFIX + "work.dirs";

  public static final String TEZ_DAEMON_YARN_SHUFFLE_PORT = TEZ_DAEMON_PREFIX + "yarn.shuffle.port";


  // Section for configs used in AM and executors
  public static final String TEZ_DAEMON_NUM_EXECUTORS = TEZ_DAEMON_PREFIX + "num.executors";
  public static final int TEZ_DAEMON_NUM_EXECUTORS_DEFAULT = 3;


  // Section for configs used in the AM //

  public static final String TEZ_DAEMON_AM_MEMORY_PER_INSTANCE_MB = TEZ_DAEMON_AM_PREFIX + "memory.per.instance.mb";
  public static final int TEZ_DAEMON_AM_MEMORY_PER_INSTANCE_MB_DEFAULT = 4096;
  public static final String TEZ_DAEMON_AM_VCPUS_PER_INSTANCE = TEZ_DAEMON_AM_PREFIX + "vcpus.per.instance";
  public static final int TEZ_DAEMON_AM_VCPUS_PER_INSTANCE_DEFAULT = 4;
  public static final String TEZ_DAEMON_AM_SERVICE_HOSTS = TEZ_DAEMON_AM_PREFIX + "service.hosts";
  public static final String TEZ_DAEMON_AM_SERVICE_HOSTS_DEFAULT = "localhost";

}
