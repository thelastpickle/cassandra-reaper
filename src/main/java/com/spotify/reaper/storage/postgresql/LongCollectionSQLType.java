package com.spotify.reaper.storage.postgresql;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.UUID;

/**
 * This is required to be able to map in generic manner into Postgres array types through JDBI.
 */
public class LongCollectionSQLType {

  private Collection<Long> collection;

  public LongCollectionSQLType(Collection<Long> collection) {
    this.collection = collection;
  }

  public Collection<Long> getValue() {
    if (this.collection == null) {
      return Lists.newArrayList();
    } else {
      return this.collection;
    }
  }
}
