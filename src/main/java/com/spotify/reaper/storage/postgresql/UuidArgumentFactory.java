package com.spotify.reaper.storage.postgresql;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.util.UUID;

/**
 * Provides JDBI a method to map UUID value to a long (most sig bits) value in database.
 */
public final class UuidArgumentFactory implements ArgumentFactory<UUID> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof UUID;
  }

  @Override
  public Argument build(Class<?> expectedType, final UUID value, StatementContext ctx) {
    return (pos, stmt, sc) -> stmt.setLong(pos, toSequenceId(value));
  }

  private static long toSequenceId(UUID id) {
    return id.getMostSignificantBits();
  }
}
