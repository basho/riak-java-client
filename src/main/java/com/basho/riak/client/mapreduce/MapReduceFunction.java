package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;

public interface MapReduceFunction {
   
   public static enum Types {
      ANONYMOUS,
      NAMED
   }
   
   public JSONObject toJson() throws JSONException;
}
