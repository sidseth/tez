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

package org.apache.tez.dag.app.launcher;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestContainerLauncherRouter {

  @Before
  @After
  public void reset() {
    ContainerLaucherRouterForMultipleLauncherTest.reset();
  }

  @Test(timeout = 5000)
  public void testNoLaunchersSpecified() throws IOException {

    AppContext appContext = mock(AppContext.class);
    TaskAttemptListener tal = mock(TaskAttemptListener.class);

    try {

      new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null, null,
          false);
      fail("Expecting a failure without any launchers being specified");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test(timeout = 5000)
  public void testCustomLauncherSpecified() throws IOException {
    Configuration conf = new Configuration(false);

    AppContext appContext = mock(AppContext.class);
    TaskAttemptListener tal = mock(TaskAttemptListener.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(1, clr.getNumContainerLaunchers());
      assertFalse(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(bb, clr.getContainerLauncherContext(0).getInitialUserPayload().getPayload());
    } finally {
      clr.stop();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleContainerLaunchers() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testvalue");
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);

    AppContext appContext = mock(AppContext.class);
    TaskAttemptListener tal = mock(TaskAttemptListener.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));
    launcherDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
            .setUserPayload(userPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(2, clr.getNumContainerLaunchers());
      assertTrue(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(bb, clr.getContainerLauncherContext(0).getInitialUserPayload().getPayload());

      assertEquals(TezConstants.getTezYarnServicePluginName(), clr.getContainerLauncherName(1));
      Configuration confParsed = TezUtils
          .createConfFromUserPayload(clr.getContainerLauncherContext(1).getInitialUserPayload());
      assertEquals("testvalue", confParsed.get("testkey"));
    } finally {
      clr.stop();
    }
  }

  @Test(timeout = 5000)
  public void testEventRouting() throws Exception {
    Configuration conf = new Configuration(false);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);

    AppContext appContext = mock(AppContext.class);
    TaskAttemptListener tal = mock(TaskAttemptListener.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));
    launcherDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
            .setUserPayload(userPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(2, clr.getNumContainerLaunchers());
      assertTrue(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(TezConstants.getTezYarnServicePluginName(), clr.getContainerLauncherName(1));

      verify(clr.getTestContainerLauncher(0)).initialize();
      verify(clr.getTestContainerLauncher(0)).start();
      verify(clr.getTestContainerLauncher(1)).initialize();
      verify(clr.getTestContainerLauncher(1)).start();

      ContainerLaunchContext clc1 = mock(ContainerLaunchContext.class);
      Container container1 = mock(Container.class);

      ContainerLaunchContext clc2 = mock(ContainerLaunchContext.class);
      Container container2 = mock(Container.class);

      NMCommunicatorLaunchRequestEvent launchRequestEvent1 =
          new NMCommunicatorLaunchRequestEvent(clc1, container1, 0, 0, 0);
      NMCommunicatorLaunchRequestEvent launchRequestEvent2 =
          new NMCommunicatorLaunchRequestEvent(clc2, container2, 1, 0, 0);

      clr.handle(launchRequestEvent1);


      ArgumentCaptor<ContainerLaunchRequest> captor =
          ArgumentCaptor.forClass(ContainerLaunchRequest.class);
      verify(clr.getTestContainerLauncher(0)).launchContainer(captor.capture());
      assertEquals(1, captor.getAllValues().size());
      ContainerLaunchRequest launchRequest1 = captor.getValue();
      assertEquals(clc1, launchRequest1.getContainerLaunchContext());

      clr.handle(launchRequestEvent2);
      captor = ArgumentCaptor.forClass(ContainerLaunchRequest.class);
      verify(clr.getTestContainerLauncher(1)).launchContainer(captor.capture());
      assertEquals(1, captor.getAllValues().size());
      ContainerLaunchRequest launchRequest2 = captor.getValue();
      assertEquals(clc2, launchRequest2.getContainerLaunchContext());

    } finally {
      clr.stop();
      verify(clr.getTestContainerLauncher(0)).shutdown();
      verify(clr.getTestContainerLauncher(1)).shutdown();
    }
  }

  private static class ContainerLaucherRouterForMultipleLauncherTest
      extends ContainerLauncherRouter {

    // All variables setup as static since methods being overridden are invoked by the ContainerLauncherRouter ctor,
    // and regular variables will not be initialized at this point.
    private static final AtomicInteger numContainerLaunchers = new AtomicInteger(0);
    private static final Set<Integer> containerLauncherIndices = new HashSet<>();
    private static final ContainerLauncher yarnContainerLauncher = mock(ContainerLauncher.class);
    private static final ContainerLauncher uberContainerlauncher = mock(ContainerLauncher.class);
    private static final AtomicBoolean yarnContainerLauncherCreated = new AtomicBoolean(false);
    private static final AtomicBoolean uberContainerLauncherCreated = new AtomicBoolean(false);

    private static final List<ContainerLauncherContext> containerLauncherContexts =
        new LinkedList<>();
    private static final List<String> containerLauncherNames = new LinkedList<>();
    private static final List<ContainerLauncher> testContainerLaunchers = new LinkedList<>();


    public static void reset() {
      numContainerLaunchers.set(0);
      containerLauncherIndices.clear();
      yarnContainerLauncherCreated.set(false);
      uberContainerLauncherCreated.set(false);
      containerLauncherContexts.clear();
      containerLauncherNames.clear();
      testContainerLaunchers.clear();
    }

    public ContainerLaucherRouterForMultipleLauncherTest(AppContext context,
                                                         TaskAttemptListener taskAttemptListener,
                                                         String workingDirectory,
                                                         List<NamedEntityDescriptor> containerLauncherDescriptors,
                                                         boolean isPureLocalMode) throws
        UnknownHostException {
      super(context, taskAttemptListener, workingDirectory,
          containerLauncherDescriptors, isPureLocalMode);
    }

    @Override
    ContainerLauncher createContainerLauncher(NamedEntityDescriptor containerLauncherDescriptor,
                                              AppContext context,
                                              ContainerLauncherContext containerLauncherContext,
                                              TaskAttemptListener taskAttemptListener,
                                              String workingDirectory,
                                              int containerLauncherIndex,
                                              boolean isPureLocalMode) {
      numContainerLaunchers.incrementAndGet();
      boolean added = containerLauncherIndices.add(containerLauncherIndex);
      assertTrue("Cannot add multiple launchers with the same index", added);
      containerLauncherNames.add(containerLauncherDescriptor.getEntityName());
      containerLauncherContexts.add(containerLauncherContext);
      return super
          .createContainerLauncher(containerLauncherDescriptor, context, containerLauncherContext,
              taskAttemptListener, workingDirectory, containerLauncherIndex, isPureLocalMode);
    }

    @Override
    ContainerLauncher createYarnContainerLauncher(
        ContainerLauncherContext containerLauncherContext) {
      yarnContainerLauncherCreated.set(true);
      testContainerLaunchers.add(yarnContainerLauncher);
      return yarnContainerLauncher;
    }

    @Override
    ContainerLauncher createUberContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                  AppContext context,
                                                  TaskAttemptListener taskAttemptListener,
                                                  String workingDirectory,
                                                  boolean isPureLocalMode) {
      uberContainerLauncherCreated.set(true);
      testContainerLaunchers.add(uberContainerlauncher);
      return uberContainerlauncher;
    }

    @Override
    ContainerLauncher createCustomContainerLauncher(
        ContainerLauncherContext containerLauncherContext,
        NamedEntityDescriptor containerLauncherDescriptor) {
      ContainerLauncher spyLauncher = spy(super.createCustomContainerLauncher(
          containerLauncherContext, containerLauncherDescriptor));
      testContainerLaunchers.add(spyLauncher);
      return spyLauncher;
    }

    public int getNumContainerLaunchers() {
      return numContainerLaunchers.get();
    }

    public boolean getYarnContainerLauncherCreated() {
      return yarnContainerLauncherCreated.get();
    }

    public boolean getUberContainerLauncherCreated() {
      return uberContainerLauncherCreated.get();
    }

    public String getContainerLauncherName(int containerLauncherIndex) {
      return containerLauncherNames.get(containerLauncherIndex);
    }

    public ContainerLauncher getTestContainerLauncher(int containerLauncherIndex) {
      return testContainerLaunchers.get(containerLauncherIndex);
    }

    public ContainerLauncherContext getContainerLauncherContext(int containerLauncherIndex) {
      return containerLauncherContexts.get(containerLauncherIndex);
    }
  }

  private static class FakeContainerLauncher extends ContainerLauncher {

    public FakeContainerLauncher(
        ContainerLauncherContext containerLauncherContext) {
      super(containerLauncherContext);
    }

    @Override
    public void launchContainer(ContainerLaunchRequest launchRequest) {

    }

    @Override
    public void stopContainer(ContainerStopRequest stopRequest) {

    }
  }

}
