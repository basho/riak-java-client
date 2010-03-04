package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;

public class LinkFunction implements MapReduceFunction {
   
   private String bucket = null;
   private String key = null;
   
   public LinkFunction(String bucket) {
      this.bucket = bucket;
   }
   
   public LinkFunction(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
   }

   public JSONObject toJson() throws JSONException {
      JSONObject link = new JSONObject();
      link.put("bucket", this.bucket);
      link.put("key", this.key);
      return link;
   }

}
