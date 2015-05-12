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

package org.apache.tez.dag.api;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

// TODO TEZ-2003 Move this into the tez-api module
public abstract class TaskCommunicator extends AbstractService {
  public TaskCommunicator(String name) {
    super(name);
  }

  // TODO TEZ-2003 Ideally, don't expose YARN containerId; instead expose a Tez specific construct.
  // TODO When talking to an external service, this plugin implementer may need access to a host:port
  public abstract void registerRunningContainer(ContainerId containerId, String hostname, int port);

  // TODO TEZ-2003 Ideally, don't expose YARN containerId; instead expose a Tez specific construct.
  public abstract void registerContainerEnd(ContainerId containerId);

  // TODO TEZ-2003 TaskSpec breakup into a clean interface
  // TODO TEZ-2003 Add support for priority
  public abstract void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                                  Map<String, LocalResource> additionalResources,
                                                  Credentials credentials,
                                                  boolean credentialsChanged, int priority);

  // TODO TEZ-2003. Are additional APIs required to mark a container as completed ? - for completeness.

  // TODO TEZ-2003 Remove reference to TaskAttemptID
  // TODO TEZ-2003 This needs some information about why the attempt is being unregistered.
  // e.g. preempted in which case the task may need to be informed. Alternately as a result of
  // a failed task.
  // In case of preemption - a killTask API is likely a better bet than trying to overload this method.
  public abstract void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID);

  // TODO TEZ-2003 This doesn't necessarily belong here. A server may not start within the AM.
  public abstract InetSocketAddress getAddress();

  // TODO Eventually. Add methods here to support preemption of tasks.

  /**
   * Receive notifications on vertex state changes.
   * <p/>
   * State changes will be received based on the registration via {@link
   * org.apache.tez.runtime.api.InputInitializerContext#registerForVertexStateUpdates(String,
   * java.util.Set)}. Notifications will be received for all registered state changes, and not just
   * for the latest state update. They will be in order in which the state change occurred. </p>
   *
   * Extensive processing should not be performed via this method call. Instead this should just be
   * used as a notification mechanism.
   * <br>This method may be invoked concurrently with other invocations into the TaskCommunicator and
   * multi-threading/concurrency implications must be considered.
   * @param stateUpdate an event indicating the name of the vertex, and it's updated state.
   *                    Additional information may be available for specific events, Look at the
   *                    type hierarchy for {@link org.apache.tez.dag.api.event.VertexStateUpdate}
   * @throws Exception
   */
  public abstract void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception;

  /**
   * Indicates the current running dag is complete. The TaskCommunicatorContext can be used to
   * query information about the current dag during the duration of the dagComplete invocation.
   *
   * After this, the contents returned from querying the context may change at any point - due to
   * the next dag being submitted.
   */
  // TODO TEZ-2003 This is extremely difficult to use. Add the dagStarted notification, and potentially
  // throw exceptions between a dagComplete and dagStart invocation.
  public abstract void dagComplete(String dagName);
}
