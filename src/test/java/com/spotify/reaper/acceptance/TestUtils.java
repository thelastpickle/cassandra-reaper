package com.spotify.reaper.acceptance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TestUtils {

  private static Cluster cluster;
  private static Session session;
 
  /** Holder */
  private static class SingletonHolder
  {   
    /** Instance unique non préinitialisée */
    private final static TestUtils instance = new TestUtils();
  }
 
  /** Point d'accès pour l'instance unique du singleton */
  public static TestUtils getInstance()
  {
    return SingletonHolder.instance;
  }
  
  private TestUtils(){
    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    session = cluster.connect();    
  }
  
  public void createKeyspace(String keyspaceName){
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication={'class':'SimpleStrategy', 'replication_factor':3}");
  }
  
  public void createTable(String keyspaceName, String tableName){
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName + "(id int PRIMARY KEY, value text)");
    for(int i=0;i<100;i++){
      session.execute("INSERT INTO " + keyspaceName + "." + tableName + "(id, value) VALUES(" + i + ",'" + i + "')");
    }
  }
  
 
}
