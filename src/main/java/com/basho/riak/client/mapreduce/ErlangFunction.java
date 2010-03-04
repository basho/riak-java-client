package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;


public class ErlangFunction implements MapReduceFunction {
   
   private String module;
   private String function;
   
   public ErlangFunction(String module, String functionName) {
      this.module = module;
      this.function = functionName;
   }
   
   public JSONObject toJson() throws JSONException {
      JSONObject retval = new JSONObject();
      retval.append("language", "erlang");
      retval.append("module", this.module);
      retval.append("function", this.function);
      return retval;
   }

}
