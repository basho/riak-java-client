package com.basho.riak.client.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents an Erlang function used in a map or reduce phase
 * of a map/reduce job
 *
 */
public class ErlangFunction implements MapReduceFunction {
   
   private String module;
   private String function;

   /**
    * Constructs a new ErlangFunction instance
    * @param module Erlang module name
    * @param functionName Erlang function name
    */
   public ErlangFunction(String module, String functionName) {
      this.module = module;
      this.function = functionName;
   }
   
   /**
    * Converts the function definition to JSON
    */
   public JSONObject toJson() throws JSONException {
      JSONObject retval = new JSONObject();
      retval.put("language", "erlang");
      retval.put("module", this.module);
      retval.put("function", this.function);
      return retval;
   }

}
