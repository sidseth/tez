/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;

public class ContainerLauncherContextImpl implements ContainerLauncherContext {

  private final AppContext context;
  private final TaskAttemptListener tal;

  public ContainerLauncherContextImpl(AppContext appContext, TaskAttemptListener tal) {
    this.context = appContext;
    this.tal = tal;
  }

  @Override
  public void containerLaunched(ContainerId containerId) {
    context.getEventHandler().handle(
        new AMContainerEventLaunched(containerId));
    ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
        containerId, context.getClock().getTime(), context.getApplicationAttemptId());
    context.getHistoryHandler().handle(new DAGHistoryEvent(
        null, lEvt));

  }

  @Override
  public void containerLaunchFailed(ContainerId containerId, String diagnostics) {
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, diagnostics));
  }

  @Override
  public void containerStopRequested(ContainerId containerId) {
    context.getEventHandler().handle(
        new AMContainerEvent(containerId, AMContainerEventType.C_NM_STOP_SENT));
  }

  @Override
  public void containerStopFailed(ContainerId containerId, String diagnostics) {
    context.getEventHandler().handle(
        new AMContainerEventStopFailed(containerId, diagnostics));
  }

  @Override
  public void containerCompleted(ContainerId containerId, int exitStatus, String diagnostics,
                                 TaskAttemptEndReason endReason) {
    context.getEventHandler().handle(new AMContainerEventCompleted(containerId, exitStatus, diagnostics, TezUtilsInternal
        .fromTaskAttemptEndReason(
            endReason)));
  }

  @Override
  public Configuration getInitialConfiguration() {
    return context.getAMConf();
  }

  @Override
  public int getNumNodes(String sourceName) {
    int sourceIndex = context.getTaskScheduerIdentifier(sourceName);
    int numNodes = context.getNodeTracker().getNumNodes(sourceIndex);
    return numNodes;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return context.getApplicationAttemptId();
  }

  @Override
  public Object getTaskCommunicatorMetaInfo(String taskCommName) {
    int taskCommId = context.getTaskCommunicatorIdentifier(taskCommName);
    return tal.getTaskCommunicator(taskCommId).getMetaInfo();
  }

}
