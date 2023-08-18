/*
 * Copyright 2023-2023 DataStax, Inc.
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


package io.cassandrareaper.management.http;

import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.management.StreamsProxy;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HttpStreamsProxy implements StreamsProxy {

  private static final Logger LOG = LoggerFactory.getLogger(HttpStreamsProxy.class);

  private final HttpCassandraManagementProxy proxy;

  HttpStreamsProxy(HttpCassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public List<StreamSession> listStreams(Node node) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
