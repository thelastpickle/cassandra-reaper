package com.spotify.reaper.storage.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;


public class DateTimeCodec extends TypeCodec<DateTime> {
  public DateTimeCodec() {
    super(DataType.timestamp(), DateTime.class);
  }

  @Override
  public DateTime parse(String value) {
    if (value == null || value.equals("NULL"))
      return null;

    try {
      return DateTime.parse(value);
    } catch (IllegalArgumentException iae) {
      throw new InvalidTypeException("Could not parse format: " + value, iae);
    }
  }

  @Override
  public String format(DateTime value) {
    if (value == null)
      return "NULL";

    return Long.toString(value.getMillis());
  }

  @Override
  public ByteBuffer serialize(DateTime value, ProtocolVersion protocolVersion) {
    return value == null ? null : BigintCodec.instance.serializeNoBoxing(value.getMillis(), protocolVersion);
  }

  @Override
  public DateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return bytes == null || bytes.remaining() == 0 ? null: new DateTime(BigintCodec.instance.deserializeNoBoxing(bytes, protocolVersion));
  }
  
  /**
   * Base class for codecs handling CQL 8-byte integer types such as {@link DataType#bigint()},
   * {@link DataType#counter()} or {@link DataType#time()}.
   */
  private abstract static class LongCodec extends PrimitiveLongCodec {

      private LongCodec(DataType cqlType) {
          super(cqlType);
      }

      @Override
      public Long parse(String value) {
          try {
              return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : Long.parseLong(value);
          } catch (NumberFormatException e) {
              throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
          }
      }

      @Override
      public String format(Long value) {
          if (value == null)
              return "NULL";
          return Long.toString(value);
      }

      @Override
      public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
          ByteBuffer bb = ByteBuffer.allocate(8);
          bb.putLong(0, value);
          return bb;
      }

      @Override
      public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
          if (bytes == null || bytes.remaining() == 0)
              return 0;
          if (bytes.remaining() != 8)
              throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

          return bytes.getLong(bytes.position());
      }
  }
  
  private static class BigintCodec extends LongCodec {

      private static final BigintCodec instance = new BigintCodec();

      private BigintCodec() {
          super(DataType.bigint());
      }

  }
}
