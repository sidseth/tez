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

import java.util.List;

import org.apache.tez.runtime.api.impl.TezEvent;

// TODO TEZ-2003 Move this into the tez-api module
public class TaskHeartbeatResponse {

  private final boolean shouldDie;
  private final int nextFromEventId;
  private final List<TezEvent> events;

  public TaskHeartbeatResponse(boolean shouldDie, List<TezEvent> events, int nextFromEventId) {
    this.shouldDie = shouldDie;
    this.events = events;
    this.nextFromEventId = nextFromEventId;
  }

  public boolean isShouldDie() {
    return shouldDie;
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public int getNextFromEventId() {
    return nextFromEventId;
  }
}
