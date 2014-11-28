package com.spotify.reaper;

public class ReaperException extends Exception {

  public ReaperException(String s) {
    super(s);
  }

  public ReaperException(Exception e) {
    super(e);
  }

  public ReaperException(String s, Exception e) {
    super(s,e);
  }
}
