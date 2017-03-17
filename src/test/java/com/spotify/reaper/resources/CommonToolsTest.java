package com.spotify.reaper.resources;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;

import java.util.Set;

public class CommonToolsTest {

  @Test
  public void testDateTimeToISO8601() {
    DateTime dateTime = new DateTime(2015, 2, 20, 15, 24, 45, DateTimeZone.UTC);
    assertEquals("2015-02-20T15:24:45Z", CommonTools.dateTimeToISO8601(dateTime));
  }
  
  @Test
  public void testParseSeedHost() {
    String seedHostStringList = "127.0.0.1 , 127.0.0.2,  127.0.0.3";
    Set<String> seedHostSet = CommonTools.parseSeedHosts(seedHostStringList);
    Set<String> seedHostExpectedSet = Sets.newHashSet("127.0.0.2","127.0.0.1","127.0.0.3");
    
    assertEquals(seedHostSet, seedHostExpectedSet);
    
  }
}