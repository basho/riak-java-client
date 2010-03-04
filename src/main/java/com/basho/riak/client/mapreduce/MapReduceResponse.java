package com.basho.riak.client.mapreduce.response;

import org.json.JSONArray;
import org.json.JSONException;

public class MapReduceResponse {

   private int status = -1;
   private String statusText;
   private String body = null;

   public MapReduceResponse(int status, String statusText, String body) {
      this.status = status;
      this.statusText = statusText;
      this.body = body;
   }
   
   public int getStatusCode() {
      return this.status;
   }
   
   public String getStatusText() {
      return this.statusText;
   }
   
   public String getBody() {
      return this.body;
   }
   
   public JSONArray getParsedBody() throws JSONException {
      return new JSONArray(this.body);
   }
   
   public boolean isSuccess() {
      return this.status >= 200 && this.status < 300; 
   }
   
   public boolean isError() {
      return !this.isSuccess();
   }
}  
