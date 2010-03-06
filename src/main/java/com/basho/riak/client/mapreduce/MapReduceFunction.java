package com.basho.riak.client.mapreduce;

import org.json.JSONObject;

/**
 * Interface for describing functions used in
 * map/reduce jobs
 */
public interface MapReduceFunction {
   
   public static enum Types {
      ANONYMOUS,
      NAMED
   }
   
   public JSONObject toJson();
}
