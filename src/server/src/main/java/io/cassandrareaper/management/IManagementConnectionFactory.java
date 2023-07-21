/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;

import java.util.Collection;
import java.util.Set;

public interface IManagementConnectionFactory {
  ICassandraManagementProxy connectAny(Collection<Node> nodes) throws ReaperException;

  //  void setAddressTranslator(AddressTranslator addressTranslator); // TODO: May not be needed for GA
  Set<String> getAccessibleDatacenters();

  HostConnectionCounters getHostConnectionCounters();
}