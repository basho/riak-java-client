package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;

public class LinkFunction implements MapReduceFunction {
   
   private String bucket = null;
   private String tag = null;
   
   public LinkFunction(String bucket) {
      this.bucket = bucket;
   }
   
   public LinkFunction(String bucket, String tag) {
      this.bucket = bucket;
      this.tag = tag;
   }

   public JSONObject toJson() {
      try {
          JSONObject link = new JSONObject();
          link.put("bucket", this.bucket);
    
          if (this.tag != null) {
             link.put("tag", this.tag);
          }

          return link;
      } catch (JSONException e) {
          throw new RuntimeException("Can always map a string to a string");
      }
   }

}
