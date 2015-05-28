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

package org.apache.tez.dag.api;

// TODO TEZ-2003 Expose as a public API
public enum ContainerEndReason {
  NODE_FAILED, // Completed because the node running the container was marked as dead
  INTERNAL_PREEMPTION, // Preempted by the AM, due to an internal decision
  EXTERNAL_PREEMPTION, // Preempted due to cluster contention
  APPLICATION_ERROR, // An error in the AM caused by user code
  FRAMEWORK_ERROR, // An error in the AM - likely a bug.
  LAUNCH_FAILED, // Failure to launch the container
  COMPLETED, // Completed via normal flow
  OTHER
}
