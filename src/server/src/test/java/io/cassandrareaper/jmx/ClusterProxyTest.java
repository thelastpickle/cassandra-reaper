/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
 *
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterProxyTest {


  @Test
  public void nodeIsAccessibleThroughJmxAllTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.clusterProxy = ClusterProxy.create(context);
    context.localNodeAddress = "127.0.0.1";
    context.localDatacenter = "dc1";
    context.localClusterName = "Test";
    context.accessibleDatacenters = new HashSet<String>(Arrays.asList("dc1"));

    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    assertTrue(context.clusterProxy.nodeIsAccessibleThroughJmx(context.localDatacenter, context.localNodeAddress));
    assertTrue(context.clusterProxy.nodeIsAccessibleThroughJmx("dc2", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxLocalTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.clusterProxy = ClusterProxy.create(context);
    context.localNodeAddress = "127.0.0.1";
    context.localDatacenter = "dc1";
    context.localClusterName = "Test";
    context.accessibleDatacenters = new HashSet<String>(Arrays.asList("dc1"));

    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);
    assertTrue(
        context.clusterProxy.nodeIsAccessibleThroughJmx(
            context.localDatacenter, context.localNodeAddress));
    assertTrue(
        context.clusterProxy.nodeIsAccessibleThroughJmx(
            "dc2", "127.0.0.2")); // it's in another DC but LOCAL allows attempting it
    assertTrue(context.clusterProxy.nodeIsAccessibleThroughJmx("dc1", "127.0.0.2")); // Should be accessible, same DC
  }

  @Test
  public void nodeIsAccessibleThroughJmxEachTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.clusterProxy = ClusterProxy.create(context);
    context.localNodeAddress = "127.0.0.1";
    context.localDatacenter = "dc1";
    context.localClusterName = "Test";
    context.accessibleDatacenters = new HashSet<String>(Arrays.asList("dc1"));

    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);
    assertTrue(
        context.clusterProxy.nodeIsAccessibleThroughJmx(
            context.localDatacenter, context.localNodeAddress));
    assertFalse(
        context.clusterProxy.nodeIsAccessibleThroughJmx(
            "dc2", "127.0.0.2")); // Should not be accessible as it's in another DC
    assertTrue(context.clusterProxy.nodeIsAccessibleThroughJmx("dc1", "127.0.0.2")); // Should be accessible, same DC
  }
}
