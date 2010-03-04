package com.basho.riak.client.mapreduce;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * Wrapper to handle map/reduce job responses
 */
public class MapReduceResponse {

   private int status = -1;
   private String statusText;
   private String body = null;

   public MapReduceResponse(int status, String statusText, String body) {
      this.status = status;
      this.statusText = statusText;
      this.body = body;
   }

   /**
    * The HTTP status code returned by the Riak server
    */
   public int getStatusCode() {
      return this.status;
   }

   /**
    * The HTTP status text returned by the Riak server
    */
   public String getStatusText() {
      return this.statusText;
   }

   /**
    * The raw String-ified body of the server response
    */
   public String getBody() {
      return this.body;
   }
   
   /**
    * Try to parse the response body and return it as JSON
    * @throws JSONException
    */
   public JSONArray getParsedBody() throws JSONException {
      return new JSONArray(this.body);
   }

   /**
    * Does the status code indicate success
    */
   public boolean isSuccess() {
      return this.status >= 200 && this.status < 300; 
   }

   /**
    * Does the status code indicate failure
    */
   public boolean isError() {
      return !this.isSuccess();
   }
}  
