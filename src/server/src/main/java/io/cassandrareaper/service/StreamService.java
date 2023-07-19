/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.management.jmx.ClusterFacade;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class StreamService {

  private static final Logger LOG = LoggerFactory.getLogger(StreamService.class);

  private final ClusterFacade clusterFacade;

  private StreamService(Supplier<ClusterFacade> clusterFacadeSupplier) {
    this.clusterFacade = clusterFacadeSupplier.get();
  }

  public static StreamService create(AppContext context) {
    return new StreamService(() -> ClusterFacade.create(context));
  }

  static StreamService create(Supplier<ClusterFacade> clusterFacadeSupplier) {
    return new StreamService(clusterFacadeSupplier);
  }

  public List<StreamSession> listStreams(Node node) throws ReaperException {
    try {
      LOG.debug("Pulling streams for node {}", node);
      return pullStreamInfo(node);
    } catch (ReaperException | InterruptedException | IOException e) {
      LOG.info("Pulling streams failed: {}", e.getMessage());
      throw new ReaperException(e);
    }
  }

  private List<StreamSession> pullStreamInfo(Node node) throws ReaperException, InterruptedException, IOException {
    return clusterFacade.listActiveStreams(node);
  }

}