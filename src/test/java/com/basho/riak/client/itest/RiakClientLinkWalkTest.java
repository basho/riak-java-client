package com.basho.riak.client.itest;

import static org.junit.Assert.*;

import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.builders.RiakObjectBuilder;

import com.basho.riak.client.query.WalkResult;

public class RiakClientLinkWalkTest {

  @Test
  public void linksCanBeWalked() throws Exception {
    IRiakClient riakClient = RiakFactory.httpClient("http://localhost:8098/riak");
    String bucketName = "myBucket";
    com.basho.riak.client.bucket.Bucket myBucket = riakClient.createBucket(bucketName).execute();

    myBucket.delete("promos1").execute();
    myBucket.delete("promo1:page=0").execute();
    myBucket.delete("promo1:page=1").execute();

    myBucket.store("promo1:page=0", "promo1:page=0 value").execute();
    myBucket.store("promo1:page=1", "promo1:page=1 value").execute();

    String tagName = "testPromoPageTag";
    IRiakObject riakObject = RiakObjectBuilder.newBuilder(bucketName, "promos1").withValue("promos1 value").addLink(bucketName, "promo1:page=0", tagName).build();
    myBucket.store(riakObject).execute();

    riakObject = myBucket.fetch("promos1").execute();
    
    WalkResult result = riakClient.walk(riakObject).addStep(bucketName, tagName).execute();
    assertTrue(result.iterator().next().size() > 0);
  }

}
