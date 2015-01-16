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

package org.apache.tez.dag.app.rm;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.daemon.TezDaemonConfiguration;
import org.apache.tez.dag.app.AppContext;


// TODO Registration with RM - so that the AM is considered dead and restarted in the expiry interval - 10 minutes.

public class DaemonTaskSchedulerService extends TaskSchedulerService {

  private static final Log LOG = LogFactory.getLog(DaemonTaskSchedulerService.class);

  private final ExecutorService appCallbackExecutor;
  private final TaskSchedulerAppCallback appClientDelegate;
  private final AppContext appContext;
  private final List<String> serviceHosts;
  private final ContainerFactory containerFactory;
  private final Random random = new Random();

  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  // Per Executor Thread
  private final Resource resourcePerExecutor;


  public DaemonTaskSchedulerService(TaskSchedulerAppCallback appClient, AppContext appContext,
                                    Configuration conf) {
    // Accepting configuration here to allow setting up fields as final
    super(DaemonTaskSchedulerService.class.getName());
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.appContext = appContext;
    this.serviceHosts = new LinkedList<String>();
    this.containerFactory = new ContainerFactory(appContext);
    this.memoryPerInstance = conf
        .getInt(TezDaemonConfiguration.TEZ_DAEMON_MEMORY_PER_INSTANCE_MB,
            TezDaemonConfiguration.TEZ_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT);
    this.coresPerInstance = conf
        .getInt(TezDaemonConfiguration.TEZ_DAEMON_VCPUS_PER_INSTANCE,
            TezDaemonConfiguration.TEZ_DAEMON_VCPUS_PER_INSTANCE_DEFAULT);
    this.executorsPerInstance = conf.getInt(TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS,
        TezDaemonConfiguration.TEZ_DAEMON_NUM_EXECUTORS_DEFAULT);

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String[] hosts = conf.getStrings(TezDaemonConfiguration.TEZ_DAEMON_AM_SERVICE_HOSTS);
    if (hosts == null || hosts.length == 0) {
      hosts = new String[]{"localhost"};
    }
    for (String host : hosts) {
      serviceHosts.add(host);
    }

    LOG.info("Running with configuration: " +
        "memoryPerInstance=" + memoryPerInstance +
        ", vcoresPerInstance=" + coresPerInstance +
        ", executorsPerInstance=" + executorsPerInstance +
        ", resourcePerInstanceInferred=" + resourcePerExecutor +
        ", hosts=" + serviceHosts.toString());

  }

  @Override
  public void serviceInit(Configuration conf) {
  }

  @Override
  public void serviceStart() {

  }

  @Override
  public void serviceStop() {
    appCallbackExecutor.shutdownNow();
  }



  @Override
  public Resource getAvailableResources() {
    // TODO This needs information about all running executors, and the amount of memory etc available across the cluster.
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public int getClusterNodeCount() {
    return serviceHosts.size();
  }

  @Override
  public void resetMatchLocalityForAllHeldContainers() {
  }

  @Override
  public Resource getTotalResources() {
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    LOG.info("DEBUG: BlacklistNode not supported");
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    LOG.info("DEBUG: unBlacklistNode not supported");
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(hosts);
    appClientDelegate.taskAllocated(task, clientCookie,
        containerFactory.createContainer(resourcePerExecutor, priority, host));
  }


  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(null);
    appClientDelegate.taskAllocated(task, clientCookie,
        containerFactory.createContainer(resourcePerExecutor, priority, host));
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded) {
    LOG.info("DEBUG: Ignoring deallocateTask for task: " + task + ", with successStatus:" + taskSucceeded);
    return true;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    LOG.info("DEBUG: Ignoring deallocateContainer for containerId: " + containerId);
    return null;
  }

  @Override
  public void setShouldUnregister() {

  }

  @Override
  public boolean hasUnregistered() {
    // Nothing to do. No registration involved.
    return true;
  }

  private ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TaskSchedulerAppCaller #%d").setDaemon(true).build());
  }

  private TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
  }

  private String selectHost(String[] requestedHosts) {
    String host = null;
    if (requestedHosts != null && requestedHosts.length > 0) {
      Arrays.sort(requestedHosts);
      host = requestedHosts[0];
      LOG.info("Selected host: " + host + " from requested hosts: " + Arrays.toString(requestedHosts));
    } else {
      host = serviceHosts.get(random.nextInt(serviceHosts.size()));
      LOG.info("Selected random host: " + host + " since the request contained no host information");
    }
    return host;
  }

  static class ContainerFactory {
    final AppContext appContext;
    AtomicInteger nextId;

    public ContainerFactory(AppContext appContext) {
      this.appContext = appContext;
      this.nextId = new AtomicInteger(1);
    }

    public Container createContainer(Resource capability, Priority priority, String hostname) {
      ApplicationAttemptId appAttemptId = appContext.getApplicationAttemptId();
      ContainerId containerId = ContainerId.newInstance(appAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance(hostname, 0);
      String nodeHttpAddress = "hostname:0";

      Container container = Container.newInstance(containerId,
          nodeId,
          nodeHttpAddress,
          capability,
          priority,
          null);

      return container;
    }
  }
}
