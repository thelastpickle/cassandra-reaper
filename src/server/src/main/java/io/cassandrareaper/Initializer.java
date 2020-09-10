/*
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to facilitate Reaper sidecar mode in Kubernetes. When
 * running Reaper as a sidecar in Kubernetes, we do not want startup to block on things
 * like connecting to the storage backend (see
 * https://github.com/thelastpickle/cassandra-reaper/issues/956 for details). Initializer
 * will instead execute tasks in a background thread. Tasks will be executed in the order
 * in which they are submitted. If a task fails, later tasks are skipped and the
 * {@link AppContext#isRunning} flag is set to false which will cause the health check to
 * report unhealthy.
 */
public class Initializer {

  private static final Logger LOG = LoggerFactory.getLogger(Initializer.class);

  private final ExecutorService tasks;

  private boolean failed;

  private final AppContext context;

  public Initializer(AppContext context) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("initializer").build();
    tasks = Executors.newSingleThreadExecutor(threadFactory);
    this.context = context;
  }

  /**
   * When {@link ReaperApplicationConfiguration#isInKubernetesSidecarMode()} is true,
   * tasks are executed in a background thread in the order submitted; otherwise, tasks
   * are executing in the calling thread. If a task fails in the background thread, later
   * tasks will not be executed.
   *
   * @param name The task name
   * @param task The task to execute
   */
  public void submit(String name, InitializationTask task) throws ReaperException, InterruptedException {
    if (context.config.isInKubernetesSidecarMode()) {
      runInBackground(name, task);
    } else {
      task.execute();
    }
  }

  private void runInBackground(String name, InitializationTask task) {
    tasks.submit(() -> {
      try {
        if (failed) {
          LOG.info("skipping initialization task {} due to previous failure", name);
        } else {
          LOG.info("executing initialization task {}", name);
          task.execute();
        }
      } catch (InterruptedException e) {
        LOG.debug("initialization task " + name + " was interrupted", e);
      } catch (RuntimeException | ReaperException e) {
        LOG.warn("initialization task " + name + " failed", e);
        failed = true;
        context.isRunning.set(false);
      }
    });
  }
}
