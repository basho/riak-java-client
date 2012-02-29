package com.basho.riak.client.cluster;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import org.junit.Test;

/**
 * pb
 */
public class ClusterTest
{
  @Test
  public void getPingFailover() throws Exception
  {
    PBClientConfig firstNode = new PBClientConfig.Builder().withHost("localhost").withPort(43087).build();
    PBClusterConfig clusterConfig = new PBClusterConfig(4);
    clusterConfig.addClient(firstNode)
            .addClient(PBClientConfig.Builder.from(firstNode).withHost("localhost").withPort(43088).build());

    IRiakClient riak=RiakFactory.newClient(clusterConfig);

    long now=System.currentTimeMillis();
    final long timeoutMillis=now+20000L;
    while(now<timeoutMillis){
      Thread.sleep(500);
      System.out.println("ping "+now);
      riak.ping();
      now=System.currentTimeMillis();
    }
  }
}
