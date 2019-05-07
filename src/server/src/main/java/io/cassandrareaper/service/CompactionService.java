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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Compaction;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.jmx.ClusterFacade;

import java.util.List;

import javax.management.JMException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class CompactionService {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

  private final AppContext context;

  private CompactionService(AppContext context) {
    this.context = context;
  }

  public static CompactionService create(AppContext context) {
    return new CompactionService(context);
  }

  public List<Compaction> listActiveCompactions(Node host) throws ReaperException {
    try {
      return ClusterFacade.create(context).listActiveCompactions(host);
    } catch (JMException | RuntimeException | InterruptedException e) {
      LOG.error("Failed listing compactions for host {}", host, e);
      throw new ReaperException(e);
    }
  }
}
