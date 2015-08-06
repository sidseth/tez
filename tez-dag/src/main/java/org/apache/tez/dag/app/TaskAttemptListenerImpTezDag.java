/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.ListUtils;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TaskCommunicator;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TaskHeartbeatRequest;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptKilled;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.common.security.JobTokenSecretManager;


@SuppressWarnings("unchecked")
@InterfaceAudience.Private
public class TaskAttemptListenerImpTezDag extends AbstractService implements
    TaskAttemptListener {

  private static final Logger LOG = LoggerFactory
      .getLogger(TaskAttemptListenerImpTezDag.class);

  private final AppContext context;
  private final TaskCommunicator[] taskCommunicators;
  private final TaskCommunicatorContext[] taskCommunicatorContexts;

  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final ContainerHeartbeatHandler containerHeartbeatHandler;

  private final TaskHeartbeatResponse RESPONSE_SHOULD_DIE = new TaskHeartbeatResponse(true, null, 0, 0);

  private final ConcurrentMap<TezTaskAttemptID, ContainerId> registeredAttempts =
      new ConcurrentHashMap<TezTaskAttemptID, ContainerId>();
  private final ConcurrentMap<ContainerId, ContainerInfo> registeredContainers =
      new ConcurrentHashMap<ContainerId, ContainerInfo>();

  // Defined primarily to work around ConcurrentMaps not accepting null values
  private static final class ContainerInfo {
    TezTaskAttemptID taskAttemptId;
    ContainerInfo(TezTaskAttemptID taskAttemptId) {
      this.taskAttemptId = taskAttemptId;
    }
  }

  private static final ContainerInfo NULL_CONTAINER_INFO = new ContainerInfo(null);


  public TaskAttemptListenerImpTezDag(AppContext context,
                                      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh,
                                      // TODO TEZ-2003 pre-merge. Remove reference to JobTokenSecretManager.
                                      JobTokenSecretManager jobTokenSecretManager,
                                      String [] taskCommunicatorClassIdentifiers,
                                      boolean isPureLocalMode) {
    super(TaskAttemptListenerImpTezDag.class.getName());
    this.context = context;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
    if (taskCommunicatorClassIdentifiers == null || taskCommunicatorClassIdentifiers.length == 0) {
      if (isPureLocalMode) {
        taskCommunicatorClassIdentifiers =
            new String[]{TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT};
      } else {
        taskCommunicatorClassIdentifiers =
            new String[]{TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT};
      }
    }
    this.taskCommunicators = new TaskCommunicator[taskCommunicatorClassIdentifiers.length];
    this.taskCommunicatorContexts = new TaskCommunicatorContext[taskCommunicatorClassIdentifiers.length];
    for (int i = 0 ; i < taskCommunicatorClassIdentifiers.length ; i++) {
      taskCommunicatorContexts[i] = new TaskCommunicatorContextImpl(context, this, i);
      taskCommunicators[i] = createTaskCommunicator(taskCommunicatorClassIdentifiers[i], i);
    }
    // TODO TEZ-2118 Start using taskCommunicator indices properly
  }

  @Override
  public void serviceStart() {
    // TODO Why is init tied to serviceStart
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      taskCommunicators[i].init(getConfig());
      taskCommunicators[i].start();
    }
  }

  @Override
  public void serviceStop() {
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      taskCommunicators[i].stop();
    }
  }

  private TaskCommunicator createTaskCommunicator(String taskCommClassIdentifier, int taskCommIndex) {
    if (taskCommClassIdentifier.equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT)) {
      LOG.info("Using Default Task Communicator");
      return createTezTaskCommunicator(taskCommunicatorContexts[taskCommIndex]);
    } else if (taskCommClassIdentifier.equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT)) {
      LOG.info("Using Default Local Task Communicator");
      return new TezLocalTaskCommunicatorImpl(taskCommunicatorContexts[taskCommIndex]);
    } else {
      LOG.info("Using TaskCommunicator: " + taskCommClassIdentifier);
      Class<? extends TaskCommunicator> taskCommClazz = (Class<? extends TaskCommunicator>) ReflectionUtils
          .getClazz(taskCommClassIdentifier);
      try {
        Constructor<? extends TaskCommunicator> ctor = taskCommClazz.getConstructor(TaskCommunicatorContext.class);
        ctor.setAccessible(true);
        return ctor.newInstance(taskCommunicatorContexts[taskCommIndex]);
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
  }

  @VisibleForTesting
  protected TezTaskCommunicatorImpl createTezTaskCommunicator(TaskCommunicatorContext context) {
    return new TezTaskCommunicatorImpl(context);
  }

  public TaskHeartbeatResponse heartbeat(TaskHeartbeatRequest request)
      throws IOException, TezException {
    ContainerId containerId = ConverterUtils.toContainerId(request
        .getContainerIdentifier());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received heartbeat from container"
          + ", request=" + request);
    }

    if (!registeredContainers.containsKey(containerId)) {
      LOG.warn("Received task heartbeat from unknown container with id: " + containerId +
          ", asking it to die");
      return RESPONSE_SHOULD_DIE;
    }

    // A heartbeat can come in anytime. The AM may have made a decision to kill a running task/container
    // meanwhile. If the decision is processed through the pipeline before the heartbeat is processed,
    // the heartbeat will be dropped. Otherwise the heartbeat will be processed - and the system
    // know how to handle this - via FailedInputEvents for example (relevant only if the heartbeat has events).
    // So - avoiding synchronization.

    pingContainerHeartbeatHandler(containerId);
    TaskAttemptEventInfo eventInfo = new TaskAttemptEventInfo(0, null, 0);
    TezTaskAttemptID taskAttemptID = request.getTaskAttemptId();
    if (taskAttemptID != null) {
      ContainerId containerIdFromMap = registeredAttempts.get(taskAttemptID);
      if (containerIdFromMap == null || !containerIdFromMap.equals(containerId)) {
        // This can happen when a task heartbeats. Meanwhile the container is unregistered.
        // The information will eventually make it through to the plugin via a corresponding unregister.
        // There's a race in that case between the unregister making it through, and this method returning.
        // TODO TEZ-2003. An exception back is likely a better approach than sending a shouldDie = true,
        // so that the plugin can handle the scenario. Alternately augment the response with error codes.
        // Error codes would be better than exceptions.
        LOG.info("Attempt: " + taskAttemptID + " is not recognized for heartbeats");
        return RESPONSE_SHOULD_DIE;
      }

      List<TezEvent> inEvents = request.getEvents();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ping from " + taskAttemptID.toString() +
            " events: " + (inEvents != null ? inEvents.size() : -1));
      }

      List<TezEvent> otherEvents = new ArrayList<TezEvent>();
      // route TASK_STATUS_UPDATE_EVENT directly to TaskAttempt and route other events
      // (DATA_MOVEMENT_EVENT, TASK_ATTEMPT_COMPLETED_EVENT, TASK_ATTEMPT_FAILED_EVENT)
      // to VertexImpl to ensure the events ordering
      //  1. DataMovementEvent is logged as RecoveryEvent before TaskAttemptFinishedEvent
      //  2. TaskStatusEvent is handled before TaskAttemptFinishedEvent
      for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
        final EventType eventType = tezEvent.getEventType();
        if (eventType == EventType.TASK_STATUS_UPDATE_EVENT) {
          TaskAttemptEvent taskAttemptEvent = new TaskAttemptEventStatusUpdate(taskAttemptID,
              (TaskStatusUpdateEvent) tezEvent.getEvent());
          context.getEventHandler().handle(taskAttemptEvent);
        } else {
          otherEvents.add(tezEvent);
        }
      }
      if(!otherEvents.isEmpty()) {
        TezVertexID vertexId = taskAttemptID.getTaskID().getVertexID();
        context.getEventHandler().handle(
            new VertexEventRouteEvent(vertexId, Collections.unmodifiableList(otherEvents)));
      }
      taskHeartbeatHandler.pinged(taskAttemptID);
      eventInfo = context
          .getCurrentDAG()
          .getVertex(taskAttemptID.getTaskID().getVertexID())
          .getTaskAttemptTezEvents(taskAttemptID, request.getStartIndex(), request.getPreRoutedStartIndex(),
              request.getMaxEvents());
    }
    return new TaskHeartbeatResponse(false, eventInfo.getEvents(), eventInfo.getNextFromEventId(), eventInfo.getNextPreRoutedFromEventId());
  }
  public void taskAlive(TezTaskAttemptID taskAttemptId) {
    taskHeartbeatHandler.pinged(taskAttemptId);
  }

  public void containerAlive(ContainerId containerId) {
    pingContainerHeartbeatHandler(containerId);
  }

  public void taskStartedRemotely(TezTaskAttemptID taskAttemptID, ContainerId containerId) {
    context.getEventHandler()
        .handle(new TaskAttemptEventStartedRemotely(taskAttemptID, containerId, null));
    pingContainerHeartbeatHandler(containerId);
  }

  public void taskKilled(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         String diagnostics) {
    // Regular flow via TaskAttempt will take care of un-registering from the heartbeat handler,
    // and messages from the scheduler will release the container.
    // TODO TEZ-2003 Maybe consider un-registering here itself, since the task is not active anymore,
    // instead of waiting for the unregister to flow through the Container.
    // Fix along the same lines as TEZ-2124 by introducing an explict context.
    context.getEventHandler().handle(new TaskAttemptEventAttemptKilled(taskAttemptId,
        diagnostics, TezUtilsInternal.fromTaskAttemptEndReason(
        taskAttemptEndReason)));
  }

  public void taskFailed(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         String diagnostics) {
    // Regular flow via TaskAttempt will take care of un-registering from the heartbeat handler,
    // and messages from the scheduler will release the container.
    // TODO TEZ-2003 Maybe consider un-registering here itself, since the task is not active anymore,
    // instead of waiting for the unregister to flow through the Container.
    // Fix along the same lines as TEZ-2124 by introducing an explict context.
    context.getEventHandler().handle(new TaskAttemptEventAttemptFailed(taskAttemptId,
        TaskAttemptEventType.TA_FAILED, diagnostics, TezUtilsInternal.fromTaskAttemptEndReason(
        taskAttemptEndReason)));
  }

  public void vertexStateUpdateNotificationReceived(VertexStateUpdate event, int taskCommIndex) throws
      Exception {
    taskCommunicators[taskCommIndex].onVertexStateUpdated(event);
  }


  /**
   * Child checking whether it can commit.
   * <p/>
   * <br/>
   * Repeatedly polls the ApplicationMaster whether it
   * {@link Task#canCommit(TezTaskAttemptID)} This is * a legacy from the
   * centralized commit protocol handling by the JobTracker.
   */
//  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
    LOG.info("Commit go/no-go request from " + taskAttemptId.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    DAG job = context.getCurrentDAG();
    Task task =
        job.getVertex(taskAttemptId.getTaskID().getVertexID()).
            getTask(taskAttemptId.getTaskID());
    return task.canCommit(taskAttemptId);
  }

  // The TaskAttemptListener register / unregister methods in this class are not thread safe.
  // The Tez framework should not invoke these methods from multiple threads.
  @Override
  public void dagComplete(DAG dag) {
    // TODO TEZ-2335. Cleanup TaskHeartbeat handler structures.
    // TODO TEZ-2345. Also cleanup attemptInfo map, so that any tasks which heartbeat are told to die.
    // Container structures remain unchanged - since they could be re-used across restarts.
    // This becomes more relevant when task kills without container kills are allowed.

    // TODO TEZ-2336. Send a signal to containers indicating DAG completion.

    // Inform all communicators of the dagCompletion.
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      ((TaskCommunicatorContextImpl)taskCommunicatorContexts[i]).dagCompleteStart(dag);
      taskCommunicators[i].dagComplete(dag.getName());
      ((TaskCommunicatorContextImpl)taskCommunicatorContexts[i]).dagCompleteEnd();
    }

  }

  @Override
  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, int taskCommId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerId: " + containerId + " registered with TaskAttemptListener");
    }
    ContainerInfo oldInfo = registeredContainers.put(containerId, NULL_CONTAINER_INFO);
    if (oldInfo != null) {
      throw new TezUncheckedException(
          "Multiple registrations for containerId: " + containerId);
    }
    NodeId nodeId = context.getAllContainers().get(containerId).getContainer().getNodeId();
    taskCommunicators[taskCommId].registerRunningContainer(containerId, nodeId.getHost(),
        nodeId.getPort());
  }

  @Override
  public void unregisterRunningContainer(ContainerId containerId, int taskCommId, ContainerEndReason endReason) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Container from TaskAttemptListener: " + containerId);
    }
    ContainerInfo containerInfo = registeredContainers.remove(containerId);
    if (containerInfo.taskAttemptId != null) {
      registeredAttempts.remove(containerInfo.taskAttemptId);
    }
    taskCommunicators[taskCommId].registerContainerEnd(containerId, endReason);
  }

  @Override
  public void registerTaskAttempt(AMContainerTask amContainerTask,
                                  ContainerId containerId, int taskCommId) {
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to unknown container: " + containerId);
    }
    if (containerInfo.taskAttemptId != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " with existing assignment to: " +
          containerInfo.taskAttemptId);
    }

    if (containerInfo.taskAttemptId != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " with existing assignment to: " +
          containerInfo.taskAttemptId);
    }

    // Explicitly putting in a new entry so that synchronization is not required on the existing element in the map.
    registeredContainers.put(containerId, new ContainerInfo(amContainerTask.getTask().getTaskAttemptID()));

    ContainerId containerIdFromMap = registeredAttempts.put(
        amContainerTask.getTask().getTaskAttemptID(), containerId);
    if (containerIdFromMap != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " when already assigned to: " + containerIdFromMap);
    }
    taskCommunicators[taskCommId].registerRunningTaskAttempt(containerId, amContainerTask.getTask(),
        amContainerTask.getAdditionalResources(), amContainerTask.getCredentials(),
        amContainerTask.haveCredentialsChanged(), amContainerTask.getPriority());
  }

  @Override
  public void unregisterTaskAttempt(TezTaskAttemptID attemptId, int taskCommId, TaskAttemptEndReason endReason) {
    ContainerId containerId = registeredAttempts.remove(attemptId);
    if (containerId == null) {
      LOG.warn("Unregister task attempt: " + attemptId + " from unknown container");
      return;
    }
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      LOG.warn("Unregister task attempt: " + attemptId +
          " from non-registered container: " + containerId);
      return;
    }
    // Explicitly putting in a new entry so that synchronization is not required on the existing element in the map.
    registeredContainers.put(containerId, NULL_CONTAINER_INFO);
    taskCommunicators[taskCommId].unregisterRunningTaskAttempt(attemptId, endReason);
  }

  @Override
  public TaskCommunicator getTaskCommunicator(int taskCommIndex) {
    return taskCommunicators[taskCommIndex];
  }

  private void pingContainerHeartbeatHandler(ContainerId containerId) {
    containerHeartbeatHandler.pinged(containerId);
  }

  private void pingContainerHeartbeatHandler(TezTaskAttemptID taskAttemptId) {
    ContainerId containerId = registeredAttempts.get(taskAttemptId);
    if (containerId != null) {
      containerHeartbeatHandler.pinged(containerId);
    } else {
      LOG.warn("Handling communication from attempt: " + taskAttemptId
          + ", ContainerId not known for this attempt");
    }
  }
}
