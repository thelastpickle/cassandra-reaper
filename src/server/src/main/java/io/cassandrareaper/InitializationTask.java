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

/**
 * Executes startup related tasks which may be run in a background thread after Reaper has
 * started. Any tasks that can block startup, e.g., connecting to Cassandra, should be run
 * as an InitializationTask.
 */
public interface InitializationTask {

  void execute() throws ReaperException, InterruptedException;

}
