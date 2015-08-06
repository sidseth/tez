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

package org.apache.tez.dag.app.launcher;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerLauncherContextImpl;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLauncherRouter extends AbstractService
    implements EventHandler<NMCommunicatorEvent> {

  static final Logger LOG = LoggerFactory.getLogger(ContainerLauncherImpl.class);

  @VisibleForTesting
  final ContainerLauncher containerLaunchers[];
  @VisibleForTesting
  final ContainerLauncherContext containerLauncherContexts[];
  protected final ServicePluginLifecycleAbstractService[] containerLauncherServiceWrappers;
  private final AppContext appContext;

  @VisibleForTesting
  public ContainerLauncherRouter(ContainerLauncher containerLauncher, AppContext context) {
    super(ContainerLauncherRouter.class.getName());
    this.appContext = context;
    containerLaunchers = new ContainerLauncher[] {containerLauncher};
    containerLauncherContexts = new ContainerLauncherContext[] {containerLauncher.getContext()};
    containerLauncherServiceWrappers = new ServicePluginLifecycleAbstractService[]{
        new ServicePluginLifecycleAbstractService(containerLauncher)};
  }

  // Accepting conf to setup final parameters, if required.
  public ContainerLauncherRouter(AppContext context,
                                 TaskAttemptListener taskAttemptListener,
                                 String workingDirectory,
                                 List<NamedEntityDescriptor> containerLauncherDescriptors,
                                 boolean isPureLocalMode) throws UnknownHostException {
    super(ContainerLauncherRouter.class.getName());

    this.appContext = context;
    Preconditions.checkArgument(
        containerLauncherDescriptors != null && !containerLauncherDescriptors.isEmpty(),
        "ContainerLauncherDescriptors must be specified");
    containerLauncherContexts = new ContainerLauncherContext[containerLauncherDescriptors.size()];
    containerLaunchers = new ContainerLauncher[containerLauncherDescriptors.size()];
    containerLauncherServiceWrappers = new ServicePluginLifecycleAbstractService[containerLauncherDescriptors.size()];


    for (int i = 0; i < containerLauncherDescriptors.size(); i++) {
      UserPayload userPayload = containerLauncherDescriptors.get(i).getUserPayload();
      ContainerLauncherContext containerLauncherContext =
          new ContainerLauncherContextImpl(context, taskAttemptListener, userPayload);
      containerLauncherContexts[i] = containerLauncherContext;
      containerLaunchers[i] = createContainerLauncher(containerLauncherDescriptors.get(i), context,
          containerLauncherContext, taskAttemptListener, workingDirectory, i, isPureLocalMode);
      containerLauncherServiceWrappers[i] = new ServicePluginLifecycleAbstractService(containerLaunchers[i]);
    }
  }

  @VisibleForTesting
  ContainerLauncher createContainerLauncher(
      NamedEntityDescriptor containerLauncherDescriptor,
      AppContext context,
      ContainerLauncherContext containerLauncherContext,
      TaskAttemptListener taskAttemptListener,
      String workingDirectory,
      int containerLauncherIndex,
      boolean isPureLocalMode) throws
      UnknownHostException {
    if (containerLauncherDescriptor.getEntityName().equals(
        TezConstants.getTezYarnServicePluginName())) {
      return createYarnContainerLauncher(containerLauncherContext);
    } else if (containerLauncherDescriptor.getEntityName()
        .equals(TezConstants.getTezUberServicePluginName())) {
      return createUberContainerLauncher(containerLauncherContext, context, taskAttemptListener,
          workingDirectory, isPureLocalMode);
    } else {
      return createCustomContainerLauncher(containerLauncherContext, containerLauncherDescriptor);
    }
  }

  @VisibleForTesting
  ContainerLauncher createYarnContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    LOG.info("Creating DefaultContainerLauncher");
    return new ContainerLauncherImpl(containerLauncherContext);
  }

  @VisibleForTesting
  ContainerLauncher createUberContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                AppContext context,
                                                TaskAttemptListener taskAttemptListener,
                                                String workingDirectory,
                                                boolean isPureLocalMode) throws
      UnknownHostException {
    LOG.info("Creating LocalContainerLauncher");
    // TODO Post TEZ-2003. LocalContainerLauncher is special cased, since it makes use of
    // extensive internals which are only available at runtime. Will likely require
    // some kind of runtime binding of parameters in the payload to work correctly.
    return
        new LocalContainerLauncher(containerLauncherContext, context, taskAttemptListener,
            workingDirectory, isPureLocalMode);
  }

  @VisibleForTesting
  ContainerLauncher createCustomContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                  NamedEntityDescriptor containerLauncherDescriptor) {
    LOG.info("Creating container launcher {}:{} ", containerLauncherDescriptor.getEntityName(),
        containerLauncherDescriptor.getClassName());
    Class<? extends ContainerLauncher> containerLauncherClazz =
        (Class<? extends ContainerLauncher>) ReflectionUtils.getClazz(
            containerLauncherDescriptor.getClassName());
    try {
      Constructor<? extends ContainerLauncher> ctor = containerLauncherClazz
          .getConstructor(ContainerLauncherContext.class);
      ctor.setAccessible(true);
      return ctor.newInstance(containerLauncherContext);
    } catch (NoSuchMethodException e) {
      throw new TezUncheckedException(e);
    } catch (InvocationTargetException e) {
      throw new TezUncheckedException(e);
    } catch (InstantiationException e) {
      throw new TezUncheckedException(e);
    } catch (IllegalAccessException e) {
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void serviceInit(Configuration conf) {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].init(conf);
    }
  }

  @Override
  public void serviceStart() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].start();
    }
  }

  @Override
  public void serviceStop() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].stop();
    }
  }

  public void dagComplete(DAG dag) {
    // Nothing required at the moment. Containers are shared across DAGs
  }

  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }


  @Override
  public void handle(NMCommunicatorEvent event) {
    int launcherId = event.getLauncherId();
    String schedulerName = appContext.getTaskSchedulerName(event.getSchedulerId());
    String taskCommName = appContext.getTaskCommunicatorName(event.getTaskCommId());
    switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        NMCommunicatorLaunchRequestEvent launchEvent = (NMCommunicatorLaunchRequestEvent) event;
        ContainerLaunchRequest launchRequest =
            new ContainerLaunchRequest(launchEvent.getNodeId(), launchEvent.getContainerId(),
                launchEvent.getContainerToken(), launchEvent.getContainerLaunchContext(),
                launchEvent.getContainer(), schedulerName,
                taskCommName);
        containerLaunchers[launcherId].launchContainer(launchRequest);
        break;
      case CONTAINER_STOP_REQUEST:
        ContainerStopRequest stopRequest =
            new ContainerStopRequest(event.getNodeId(), event.getContainerId(),
                event.getContainerToken(), schedulerName, taskCommName);
        containerLaunchers[launcherId].stopContainer(stopRequest);
        break;
    }
  }
}
