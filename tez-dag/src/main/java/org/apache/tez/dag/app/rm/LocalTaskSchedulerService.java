/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import com.google.common.primitives.Ints;

import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.common.ContainerSignatureMatcher;

public class LocalTaskSchedulerService extends TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTaskSchedulerService.class);

  final ContainerSignatureMatcher containerSignatureMatcher;
  final PriorityBlockingQueue<TaskRequest> taskRequestQueue;
  final Configuration conf;
  AsyncDelegateRequestHandler taskRequestHandler;
  Thread asyncDelegateRequestThread;

  final HashMap<Object, Container> taskAllocations;
  final String appTrackingUrl;
  final long customContainerAppId;

  public LocalTaskSchedulerService(TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
    taskRequestQueue = new PriorityBlockingQueue<TaskRequest>();
    taskAllocations = new LinkedHashMap<Object, Container>();
    this.appTrackingUrl = taskSchedulerContext.getAppTrackingUrl();
    this.containerSignatureMatcher = taskSchedulerContext.getContainerSignatureMatcher();
    this.customContainerAppId = taskSchedulerContext.getCustomClusterIdentifier();
    this.conf = taskSchedulerContext.getInitialConfiguration();
  }

  @Override
  public Resource getAvailableResources() {
    long memory = Runtime.getRuntime().freeMemory();
    int cores = Runtime.getRuntime().availableProcessors();
    return createResource(memory, cores);
  }

  static Resource createResource(long runtimeMemory, int core) {
    if (runtimeMemory < 0 || core < 0) {
      throw new IllegalArgumentException("Negative Memory or Core provided!"
          + "mem: "+runtimeMemory+" core:"+core);
    }
    return Resource.newInstance(Ints.checkedCast(runtimeMemory/(1024*1024)), core);
  }

  @Override
  public int getClusterNodeCount() {
    return 1;
  }

  @Override
  public void dagComplete() {
  }

  @Override
  public Resource getTotalResources() {
    long memory = Runtime.getRuntime().maxMemory();
    int cores = Runtime.getRuntime().availableProcessors();
    return createResource(memory, cores);
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts,
      String[] racks, Priority priority, Object containerSignature,
      Object clientCookie) {
    taskRequestHandler.addAllocateTaskRequest(task, capability, priority, clientCookie);
  }

  @Override
  public synchronized void allocateTask(Object task, Resource capability,
      ContainerId containerId, Priority priority, Object containerSignature,
      Object clientCookie) {
    // in local mode every task is already container level local
    taskRequestHandler.addAllocateTaskRequest(task, capability, priority, clientCookie);
  }
  
  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason) {
    return taskRequestHandler.addDeallocateTaskRequest(task);
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public void initialize() {
    taskRequestHandler = createRequestHandler(conf);
    asyncDelegateRequestThread = new Thread(taskRequestHandler);
    asyncDelegateRequestThread.setDaemon(true);
  }

  protected AsyncDelegateRequestHandler createRequestHandler(Configuration conf) {
    return new AsyncDelegateRequestHandler(taskRequestQueue,
        new LocalContainerFactory(getContext().getApplicationAttemptId(), customContainerAppId),
        taskAllocations,
        getContext(),
        conf);
  }

  @Override
  public void start() {
    asyncDelegateRequestThread.start();
  }

  @Override
  public void shutdown() throws InterruptedException {
    if (asyncDelegateRequestThread != null) {
      asyncDelegateRequestThread.interrupt();
    }
  }

  @Override
  public void setShouldUnregister() {
  }

  @Override
  public boolean hasUnregistered() {
    // Should always return true as no multiple attempts in local mode
    return true;
  }

  static class LocalContainerFactory {
    AtomicInteger nextId;
    final ApplicationAttemptId customAppAttemptId;

    public LocalContainerFactory(ApplicationAttemptId appAttemptId, long customAppId) {
      this.nextId = new AtomicInteger(1);
      ApplicationId appId = ApplicationId
          .newInstance(customAppId, appAttemptId.getApplicationId().getId());
      this.customAppAttemptId = ApplicationAttemptId
          .newInstance(appId, appAttemptId.getAttemptId());
    }

    public Container createContainer(Resource capability, Priority priority) {
      ContainerId containerId = ContainerId.newInstance(customAppAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance("127.0.0.1", 0);
      String nodeHttpAddress = "127.0.0.1:0";

      Container container = Container.newInstance(containerId,
          nodeId,
          nodeHttpAddress,
          capability,
          priority,
          null);

      return container;
    }
  }

  static class TaskRequest implements Comparable<TaskRequest> {
    // Higher prority than Priority.UNDEFINED
    static final int HIGHEST_PRIORITY = -2;
    Object task;
    Priority priority;

    public TaskRequest(Object task, Priority priority) {
      this.task = task;
      this.priority = priority;
    }

    @Override
    public int compareTo(TaskRequest request) {
      return request.priority.compareTo(this.priority);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TaskRequest that = (TaskRequest) o;

      if (priority != null ? !priority.equals(that.priority) : that.priority != null) {
        return false;
      }
      if (task != null ? !task.equals(that.task) : that.task != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 7841 * result + (task != null ? task.hashCode() : 0);
      result = 7841 * result + (priority != null ? priority.hashCode() : 0);
      return result;
    }

  }

  static class AllocateTaskRequest extends TaskRequest {
    Resource capability;
    Object clientCookie;

    public AllocateTaskRequest(Object task, Resource capability, Priority priority,
        Object clientCookie) {
      super(task, priority);
      this.capability = capability;
      this.clientCookie = clientCookie;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      AllocateTaskRequest that = (AllocateTaskRequest) o;

      if (capability != null ? !capability.equals(that.capability) : that.capability != null) {
        return false;
      }
      if (clientCookie != null ? !clientCookie.equals(that.clientCookie) :
          that.clientCookie != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 12329 * result + (capability != null ? capability.hashCode() : 0);
      result = 12329 * result + (clientCookie != null ? clientCookie.hashCode() : 0);
      return result;
    }
  }

  static class DeallocateTaskRequest extends TaskRequest {
    static final Priority DEALLOCATE_PRIORITY = Priority.newInstance(HIGHEST_PRIORITY);

    public DeallocateTaskRequest(Object task) {
      super(task, DEALLOCATE_PRIORITY);
    }
  }

  static class AsyncDelegateRequestHandler implements Runnable {
    final BlockingQueue<TaskRequest> taskRequestQueue;
    final LocalContainerFactory localContainerFactory;
    final HashMap<Object, Container> taskAllocations;
    final TaskSchedulerContext taskSchedulerContext;
    final int MAX_TASKS;

    AsyncDelegateRequestHandler(BlockingQueue<TaskRequest> taskRequestQueue,
        LocalContainerFactory localContainerFactory,
        HashMap<Object, Container> taskAllocations,
        TaskSchedulerContext taskSchedulerContext,
        Configuration conf) {
      this.taskRequestQueue = taskRequestQueue;
      this.localContainerFactory = localContainerFactory;
      this.taskAllocations = taskAllocations;
      this.taskSchedulerContext = taskSchedulerContext;
      this.MAX_TASKS = conf.getInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS,
          TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT);
    }

    public void addAllocateTaskRequest(Object task, Resource capability, Priority priority,
        Object clientCookie) {
      try {
        taskRequestQueue.put(new AllocateTaskRequest(task, capability, priority, clientCookie));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean addDeallocateTaskRequest(Object task) {
      try {
        taskRequestQueue.put(new DeallocateTaskRequest(task));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      synchronized(taskRequestQueue) {
        taskRequestQueue.notify();
      }
      return true;
    }

    boolean shouldWait() {
      return taskAllocations.size() >= MAX_TASKS;
    }

    @Override
    public void run() {
      while(!Thread.currentThread().isInterrupted()) {
        synchronized(taskRequestQueue) {
          try {
            if (shouldWait()) {
              taskRequestQueue.wait();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        processRequest();
      }
    }

    void processRequest() {
        try {
          TaskRequest request = taskRequestQueue.take();
          if (request instanceof AllocateTaskRequest) {
            allocateTask((AllocateTaskRequest)request);
          }
          else if (request instanceof DeallocateTaskRequest) {
            deallocateTask((DeallocateTaskRequest)request);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (NullPointerException e) {
          LOG.warn("Task request was badly constructed");
        }
    }

    void allocateTask(AllocateTaskRequest request) {
      Container container = localContainerFactory.createContainer(request.capability,
          request.priority);
      taskAllocations.put(request.task, container);
      taskSchedulerContext.taskAllocated(request.task, request.clientCookie, container);
    }

    void deallocateTask(DeallocateTaskRequest request) {
      Container container = taskAllocations.remove(request.task);
      if (container != null) {
        taskSchedulerContext.containerBeingReleased(container.getId());
      }
      else {
        boolean deallocationBeforeAllocation = false;
        Iterator<TaskRequest> iter = taskRequestQueue.iterator();
        while (iter.hasNext()) {
          TaskRequest taskRequest = iter.next();
          if (taskRequest instanceof AllocateTaskRequest && taskRequest.task.equals(request.task)) {
            iter.remove();
            deallocationBeforeAllocation = true;
            LOG.info("deallcation happen before allocation for task:" + request.task);
            break;
          }
        }
        if (!deallocationBeforeAllocation) {
          throw new TezUncheckedException("Unable to find and remove task " + request.task + " from task allocations");
        }
      }
    }
  }
}
