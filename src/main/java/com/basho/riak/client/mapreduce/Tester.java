package com.basho.riak.client.mapreduce;

import org.json.JSONArray;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.mapreduce.response.MapReduceResponse;


public class Tester {
   
   public static void main(String[] argv) throws Exception {
      
      RiakClient mapredClient = new RiakClient("http://localhost:8098/mapred");      
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false).reduce(JavascriptFunction.named("Riak.reduceMin"), true);
      builder.setTimeout(60000);
      builder.setBucket("mr_test");
      MapReduceResponse response = builder.submit(mapredClient);
      if (response.isSuccess()) {
         JSONArray results = response.getParsedBody();
      }
   }
}
