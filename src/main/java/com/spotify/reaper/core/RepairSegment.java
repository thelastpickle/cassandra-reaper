package com.spotify.reaper.core;

import java.math.BigInteger;

public class RepairSegment {
  private long id;
  private BigInteger startToken;
  private BigInteger endToken;

  public RepairSegment(BigInteger start, BigInteger end) {
    this.startToken = start;
    this.endToken = end;
  }

  @Override
  public String toString() {
    return String.format("(%s,%s)", startToken.toString(), endToken.toString());
  }

}
