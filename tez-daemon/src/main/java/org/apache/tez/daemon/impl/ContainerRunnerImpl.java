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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.daemon.ContainerRunner;
import org.apache.tez.daemon.rpc.TezDaemonProtocolProtos.RunContainerRequest;

public class ContainerRunnerImpl extends AbstractService implements ContainerRunner {

  private final int numExecutors;
  private final ListeningExecutorService executorService;
  // TODO Support for removing queued containers, interrupting / killing specific containers

  public ContainerRunnerImpl(int numExecutors) {
    super("ContainerRunnerImpl");
    this.numExecutors = numExecutors;

    ExecutorService raw = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setNameFormat("ContainerExecutor %d").build());
    this.executorService = MoreExecutors.listeningDecorator(raw);
  }

  @Override
  public void serviceInit(Configuration conf) {
  }

  @Override
  public void serviceStart() {
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }


  @Override
  public void queueContainer(RunContainerRequest request) {
    ListenableFuture<Integer> future = executorService.submit(new ContainerRunnerCallable(request));
    // TODO Futures.addCallback
  }

  static class ContainerRunnerCallable implements Callable<Integer> {

    private final RunContainerRequest request;

    ContainerRunnerCallable(RunContainerRequest request) {
      this.request = request;
    }

    @Override
    public Integer call() throws Exception {
      return null;
    }
  }

}