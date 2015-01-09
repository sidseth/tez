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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.daemon.ContainerRunner;
import org.apache.tez.daemon.TezDaemonConfiguration;
import org.apache.tez.daemon.TezDaemonProtocolBlockingPB;
import org.apache.tez.daemon.rpc.TezDaemonProtocolProtos;

public class TezDaemonProtocolServerImpl extends AbstractService implements TezDaemonProtocolBlockingPB {

  private static final Log LOG = LogFactory.getLog(TezDaemonProtocolServerImpl.class);

  private final TezDaemonConfiguration daemonConf;
  private final ContainerRunner containerRunner;
  private RPC.Server server;
  private InetSocketAddress bindAddress;


  public TezDaemonProtocolServerImpl(TezDaemonConfiguration daemonConf, ContainerRunner containerRunner) {
    super("TezDaemonProtocolServerImpl");
    this.daemonConf = daemonConf;
    this.containerRunner = containerRunner;
  }

  @Override
  public TezDaemonProtocolProtos.RunContainerResponse runContainer(RpcController controller,
                                                                   TezDaemonProtocolProtos.RunContainerRequest request) throws
      ServiceException {
    LOG.info("Received request: " + request);
    containerRunner.queueContainer(request);
    return TezDaemonProtocolProtos.RunContainerResponse.getDefaultInstance();
  }



  @Override
  public void serviceStart() {
    Configuration conf = getConfig();

    int numHandlers = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_RPC_NUM_HANDLERS, TezDaemonConfiguration.TEZ_DAEMON_RPC_NUM_HANDLERS_DEFAULT);
    int port = daemonConf.getInt(TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT, TezDaemonConfiguration.TEZ_DAEMON_RPC_PORT_DEFAULT);
    InetSocketAddress addr = new InetSocketAddress(port);
    LOG.info("Attempting to start TezDaemonProtocol on port=" + port + ", with numHandlers=" + numHandlers);

    try {
      server = createServer(TezDaemonProtocolBlockingPB.class, addr, conf, numHandlers,
          TezDaemonProtocolProtos.TezDaemonProtocol.newReflectiveBlockingService(this));
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to run RPC Server on port: " + port, e);
      throw new RuntimeException(e);
    }

    InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
    this.bindAddress = NetUtils.createSocketAddrForHost(
        serverBindAddress.getAddress().getCanonicalHostName(),
        serverBindAddress.getPort());
    LOG.info("Instantiated TezDaemonProtocol at " + bindAddress);
  }

  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  private RPC.Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf, int numHandlers, BlockingService blockingService) throws
      IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Server server = new RPC.Builder(conf)
        .setProtocol(pbProtocol)
        .setInstance(blockingService)
        .setBindAddress(addr.getHostName())
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers)
        .build();
    // TODO Add security.
    return server;
  }
}
