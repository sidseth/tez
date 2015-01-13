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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.log4j.Logger;
import org.apache.tez.daemon.ContainerRunner;
import org.apache.tez.daemon.rpc.TezDaemonProtocolProtos.RunContainerRequest;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.task.TezChild;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult;

public class ContainerRunnerImpl extends AbstractService implements ContainerRunner {

  private static final Logger LOG = Logger.getLogger(ContainerRunnerImpl.class);

  private final int numExecutors;
  private final ListeningExecutorService executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final String[] localDirsBase;
  private final int localShufflePort;
  private final Map<String, String> localEnv = new HashMap<String, String>();
  private final String processUser;
  private volatile FileSystem localFs;
  // TODO Support for removing queued containers, interrupting / killing specific containers

  public ContainerRunnerImpl(int numExecutors, String[] localDirsBase, int localShufflePort,
                             AtomicReference<InetSocketAddress> localAddress, String processUser) {
    super("ContainerRunnerImpl");
    this.numExecutors = numExecutors;
    this.localDirsBase = localDirsBase;
    this.localShufflePort = localShufflePort;
    this.localAddress = localAddress;
    this.processUser = processUser;

    ExecutorService raw = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setNameFormat("ContainerExecutor %d").build());
    this.executorService = MoreExecutors.listeningDecorator(raw);
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID, ByteBuffer.allocate(4).putInt(localShufflePort), localEnv);
  }

  @Override
  public void serviceInit(Configuration conf) {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
  }

  @Override
  public void serviceStart() {
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  // TODO Move this into a utilities class
  private static String createAppSpecificLocalDir(String baseDir, String applicationIdString,
                                                  String user) {
    // TODO This is broken for secure clusters. The app will not have permission to create these directories.
    // May work via Slider - since the directory would already exist. Otherwise may need a custom shuffle handler.
    // TODO This should be the process user - and not the user on behalf of whom the query is being submitted.
    return baseDir + File.separator + "usercache" + File.separator + user + File.separator +
        "appcache" + File.separator + applicationIdString;
  }

  @Override
  public void queueContainer(RunContainerRequest request) throws IOException {

    Map<String, String> env = new HashMap<String, String>();
    // TODO What else is required in this environment map.
    env.putAll(localEnv);
    env.put(ApplicationConstants.Environment.USER.name(), request.getUser());
    String[] localDirs = new String[localDirsBase.length];

    // Setup up local dirs to be application specific, and create them.
    for (int i = 0 ; i < localDirsBase.length ; i++) {
      localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], request.getApplicationIdString(), processUser);
      localFs.mkdirs(new Path(localDirs[i]));
    }
    LOG.info("DEBUG: Dirs are: " + Arrays.toString(localDirs));


    // Setup workingDir. This is otherwise setup as Environment.PWD
    // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)
    // TODO Set this up to read user configuration if required. Ideally, Inputs / Outputs should be self configured.
    // Setting this up correctly is more from framework components to setup security, ping intervals, etc.
    String workingDir = localDirs[0];


    ListenableFuture<ContainerExecutionResult> future = executorService
        .submit(new ContainerRunnerCallable(request, new Configuration(getConfig()),
            new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
            workingDir));
    Futures.addCallback(future, new ContainerRunnerCallback(request));
  }

  static class ContainerRunnerCallable implements Callable<ContainerExecutionResult> {

    private final RunContainerRequest request;
    private final Configuration conf;
    private final String workingDir;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    // TODO Is a null pid valid - will this work with multiple different ResourceMonitors ?
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;


    ContainerRunnerCallable(RunContainerRequest request, Configuration conf,
                            ExecutionContext executionContext, Map<String, String> envMap,
                            String[] localDirs, String workingDir) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.workingDir = workingDir;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
    }

    @Override
    public ContainerExecutionResult call() throws Exception {
      TezChild tezChild =
          new TezChild(conf, request.getAmHost(), request.getAmPort(), request.getContainerIdString(),
              request.getTokenIdentifier(), request.getAppAttemptNumber(), workingDir, localDirs, envMap, objectRegistry, pid,
              executionContext);
      return tezChild.run();
    }
  }

  final class ContainerRunnerCallback implements FutureCallback<ContainerExecutionResult> {

    private final RunContainerRequest request;

    ContainerRunnerCallback(RunContainerRequest request) {
      this.request = request;
    }

    // TODO Slightly more useful error handling
    @Override
    public void onSuccess(ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + request.getApplicationIdString() + ", containerId=" + request.getContainerIdString());
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + request.getApplicationIdString() + ", containerId=" + request.getContainerIdString(), result.getThrowable());
          break;
        case INTERRUPTED:
          LOG.info("Interrupted while running: " + request.getApplicationIdString() + ", containerId=" + request.getContainerIdString(), result.getThrowable());
          break;
        case ASKED_TO_DIE:
          LOG.info("Asked to die while running: " + request.getApplicationIdString() + ", containerId=" + request.getContainerIdString());
          break;
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error(
          "TezChild execution failed for : " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString());
    }
  }
}