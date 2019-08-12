/*
 * Copyright 2019-2019 The Last Pickle Ltd
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
import io.cassandrareaper.core.StreamSession;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterFacadeTest {

  @Test
  public void nodeIsAccessibleThroughJmxSidecarTest() throws ReaperException {
    final AppContext cxt = new AppContext();
    cxt.config = new ReaperApplicationConfiguration();
    AppContext contextSpy = Mockito.spy(cxt);
    Mockito.doReturn("127.0.0.1").when(contextSpy).getLocalNodeAddress();

    contextSpy.config.setDatacenterAvailability(DatacenterAvailability.SIDECAR);
    JmxConnectionFactory jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(jmxConnectionFactory.getAccessibleDatacenters()).thenReturn(new HashSet<String>(Arrays.asList("dc1")));
    contextSpy.jmxConnectionFactory = jmxConnectionFactory;
    ClusterFacade clusterFacade = ClusterFacade.create(contextSpy);
    assertTrue(clusterFacade.nodeIsAccessibleThroughJmx("dc1", contextSpy.getLocalNodeAddress()));
    assertFalse(clusterFacade.nodeIsAccessibleThroughJmx("dc1", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxAllTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito.when(context.jmxConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    assertTrue( ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc1", "127.0.0.1"));
    assertTrue(ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc2", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxLocalTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito.when(context.jmxConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);
    // it's in another DC so LOCAL disallows attempting it
    assertFalse(ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc2", "127.0.0.2"));
    // Should be accessible, same DC
    assertTrue(ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc1", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxEachTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito.when(context.jmxConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);
    // Should not be accessible as it's in another DC
    assertFalse(ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc2", "127.0.0.2"));
    // Should be accessible, same DC
    assertTrue(ClusterFacade.create(context).nodeIsAccessibleThroughJmx("dc1", "127.0.0.2"));
  }

  @Test
  public void parseStreamSessionJsonTest() throws IOException {
    URL url = Resources.getResource("metric-samples/stream-session.json");
    String data = Resources.toString(url, Charsets.UTF_8);
    List<StreamSession> list = ClusterFacade.parseStreamSessionJson(data);

    assertEquals("6b9d35b0-bab6-11e9-8e34-4d2f1172e8bc", list.get(0).getPlanId());
  }
}
