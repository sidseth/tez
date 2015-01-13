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
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.Logger;
import org.apache.tez.daemon.ContainerRunner;
import org.apache.tez.daemon.TezDaemonConfiguration;
import org.apache.tez.daemon.rpc.TezDaemonProtocolProtos;

public class TezDaemon extends AbstractService implements ContainerRunner {

  private static final Logger LOG = Logger.getLogger(TezDaemon.class);

  private final TezDaemonConfiguration daemonConf;
  private final int numExecutors;
  private final int rpcPort;
  private final TezDaemonProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;
  private final String[] localDirs;
  private final int shufflePort;
  // TODO Not the best way to share the address
  private final AtomicReference<InetSocketAddress> address = new AtomicReference<InetSocketAddress>();

  private final BlockingQueue<TezDaemonProtocolProtos.RunContainerRequest> pendingContainers =
      new LinkedBlockingQueue<TezDaemonProtocolProtos.RunContainerRequest>();

  public TezDaemon(TezDaemonConfiguration daemonConf) {
    super("TezDaemon");
    this.numExecutors = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS,
        TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.rpcPort = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT,
        TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT_DEFAULT);
    this.daemonConf = daemonConf;
    this.localDirs = daemonConf.getStrings(TezDaemonConfiguration.TEZ_DAEMON_WORK_DIRS);
    this.shufflePort = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_YARN_SHUFFLE_PORT, -1);

    LOG.info("TezDaemon started with the following configuration: " +
        "numExecutors=" + numExecutors +
        ", rpcListenerPort=" + rpcPort +
        ", workDirs=" + Arrays.toString(localDirs) +
        ", shufflePort=" + shufflePort);

    this.server = new TezDaemonProtocolServerImpl(daemonConf, this, address);
    this.containerRunner = new ContainerRunnerImpl(numExecutors, localDirs, shufflePort, address, System.getenv("user.name"));
  }

  @Override
  public void serviceInit(Configuration conf) {
    server.init(conf);
    containerRunner.init(conf);
  }

  @Override
  public void serviceStart() {
    server.start();
    containerRunner.start();

  }

  public void serviceStop() {
    containerRunner.stop();
    server.stop();
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


  @Override
  public void queueContainer(TezDaemonProtocolProtos.RunContainerRequest request) throws IOException {
    containerRunner.queueContainer(request);
  }
}
