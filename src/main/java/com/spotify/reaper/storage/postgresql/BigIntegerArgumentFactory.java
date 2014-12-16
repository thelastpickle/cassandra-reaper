package com.spotify.reaper.storage.postgresql;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Provides JDBI a method to map BigInteger value to a BIGINT value in database.
 */
public class BigIntegerArgumentFactory implements ArgumentFactory<BigInteger> {

  private static final Logger LOG = LoggerFactory.getLogger(BigIntegerArgumentFactory.class);

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof BigInteger;
  }

  @Override
  public Argument build(Class<?> expectedType, final BigInteger value, StatementContext ctx) {
    return new Argument() {
      public void apply(int position,
                        PreparedStatement statement,
                        StatementContext ctx) throws SQLException {
        statement.setBigDecimal(position, new BigDecimal(value));
      }
    };
  }
}
