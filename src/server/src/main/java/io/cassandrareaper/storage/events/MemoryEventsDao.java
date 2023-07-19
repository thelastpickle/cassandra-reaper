/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.events;

import io.cassandrareaper.core.DiagEventSubscription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class MemoryEventsDao implements IEventsDao {
  private final ConcurrentMap<UUID, DiagEventSubscription> subscriptionsById = Maps.newConcurrentMap();

  public MemoryEventsDao() {

  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return subscriptionsById.values();
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    Preconditions.checkNotNull(clusterName);
    Collection<DiagEventSubscription> ret = new ArrayList<DiagEventSubscription>();
    for (DiagEventSubscription sub : subscriptionsById.values()) {
      if (sub.getCluster().equals(clusterName)) {
        ret.add(sub);
      }
    }
    return ret;
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    if (subscriptionsById.containsKey(id)) {
      return subscriptionsById.get(id);
    }
    throw new IllegalArgumentException("No event subscription with id " + id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    Preconditions.checkArgument(subscription.getId().isPresent());
    subscriptionsById.put(subscription.getId().get(), subscription);
    return subscription;
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    return subscriptionsById.remove(id) != null;
  }
}