package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;


public class JavascriptFunction implements MapReduceFunction {
   
   private String source;
   private MapReduceFunction.Types type;
   
   public static JavascriptFunction named(String functionSource) {
      return new JavascriptFunction(MapReduceFunction.Types.NAMED, functionSource);
   }
   
   public static JavascriptFunction anon(String functionSource) {
      return new JavascriptFunction(MapReduceFunction.Types.ANONYMOUS, functionSource);
   }
   
   public JavascriptFunction(MapReduceFunction.Types functionType,
                                             String functionSource) {
      this.type = functionType;
      this.source = functionSource; 
   }
   
   public JSONObject toJson() throws JSONException {
      JSONObject retval = new JSONObject();
      retval.put("language", "javascript");
      if (type == MapReduceFunction.Types.NAMED) {
         retval.put("name", this.source);
      }
      else {
         retval.put("source", this.source);
      }
      return retval;
   }

}
