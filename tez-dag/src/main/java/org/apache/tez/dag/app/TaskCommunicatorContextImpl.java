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

package org.apache.tez.dag.app;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TaskHeartbeatRequest;
import org.apache.tez.dag.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.dag.VertexStateUpdateListener;
import org.apache.tez.dag.records.TezTaskAttemptID;

@InterfaceAudience.Private
public class TaskCommunicatorContextImpl implements TaskCommunicatorContext, VertexStateUpdateListener {


  private final AppContext context;
  private final TaskAttemptListenerImpTezDag taskAttemptListener;
  private final int taskCommunicatorIndex;

  public TaskCommunicatorContextImpl(AppContext appContext,
                                     TaskAttemptListenerImpTezDag taskAttemptListener,
                                     int taskCommunicatorIndex) {
    this.context = appContext;
    this.taskAttemptListener = taskAttemptListener;
    this.taskCommunicatorIndex = taskCommunicatorIndex;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return context.getApplicationAttemptId();
  }

  @Override
  public Credentials getCredentials() {
    return context.getAppCredentials();
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
    return taskAttemptListener.canCommit(taskAttemptId);
  }

  @Override
  public TaskHeartbeatResponse heartbeat(TaskHeartbeatRequest request) throws IOException,
      TezException {
    return taskAttemptListener.heartbeat(request);
  }

  @Override
  public boolean isKnownContainer(ContainerId containerId) {
    return context.getAllContainers().get(containerId) != null;
  }

  @Override
  public void taskAlive(TezTaskAttemptID taskAttemptId) {
    taskAttemptListener.taskAlive(taskAttemptId);
  }

  @Override
  public void containerAlive(ContainerId containerId) {
    taskAttemptListener.containerAlive(containerId);
  }

  @Override
  public void taskStartedRemotely(TezTaskAttemptID taskAttemptId, ContainerId containerId) {
    taskAttemptListener.taskStartedRemotely(taskAttemptId, containerId);
  }

  @Override
  public void taskKilled(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         @Nullable String diagnostics) {
    taskAttemptListener.taskKilled(taskAttemptId, taskAttemptEndReason, diagnostics);
  }

  @Override
  public void taskFailed(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         @Nullable String diagnostics) {
    taskAttemptListener.taskFailed(taskAttemptId, taskAttemptEndReason, diagnostics);

  }

  @Override
  public void registerForVertexStateUpdates(String vertexName,
                                            @Nullable Set<VertexState> stateSet) {
    Preconditions.checkNotNull(vertexName, "VertexName cannot be null: " + vertexName);
    context.getCurrentDAG().getStateChangeNotifier().registerForVertexUpdates(vertexName, stateSet, this);
  }


  @Override
  public void onStateUpdated(VertexStateUpdate event) {
    try {
      taskAttemptListener.vertexStateUpdateNotificationReceived(event, taskCommunicatorIndex);
    } catch (Exception e) {
      // TODO TEZ-2003 This needs to be propagated to the DAG as a user error.
      throw new TezUncheckedException(e);
    }
  }
}
