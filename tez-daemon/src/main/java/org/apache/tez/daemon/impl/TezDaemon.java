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

package org.apache.tez.daemon.impl;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.Logger;
import org.apache.tez.daemon.TezDaemonConfiguration;
import org.apache.tez.daemon.rpc.TezDaemonProtocolProtos;

public class TezDaemon extends AbstractService {

  private static final Logger LOG = Logger.getLogger(TezDaemon.class);

  private final TezDaemonConfiguration daemonConf;
  private final int numExecutors;
  private final int rpcPort;
  private final TezDaemonProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;

  private final BlockingQueue<TezDaemonProtocolProtos.RunContainerRequest> pendingContainers =
      new LinkedBlockingQueue<TezDaemonProtocolProtos.RunContainerRequest>();

  public TezDaemon(TezDaemonConfiguration daemonConf) {
    super("TezDaemon");
    this.numExecutors = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS, TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.rpcPort = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT, TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT_DEFAULT);
    this.daemonConf = daemonConf;
    this.containerRunner = new ContainerRunnerImpl(numExecutors);
    this.server = new TezDaemonProtocolServerImpl(daemonConf, containerRunner);
  }

  @Override
  public void serviceInit(Configuration conf) {
    containerRunner.init(conf);
    server.init(conf);
  }

  @Override
  public void serviceStart() {
    containerRunner.start();
    server.start();
  }

  public void serviceStop() {
    server.stop();
    containerRunner.stop();
  }


  public static void main(String[] args) throws IOException {
    TezDaemonConfiguration daemonConf = new TezDaemonConfiguration();
    TezDaemon tezDaemon = new TezDaemon(daemonConf);
    // TODO Get the PID - FWIW

    tezDaemon.init(new Configuration());
    tezDaemon.start();
    LOG.info("Started TezDaemon");
    // Relying on the RPC threads to keep the service alive.
  }


}
