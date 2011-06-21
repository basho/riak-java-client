/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.http.mapreduce;

import static org.junit.Assert.assertEquals;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.http.mapreduce.ErlangFunction;
import com.basho.riak.client.http.mapreduce.JavascriptFunction;

public class TestMapReduceFunctions {

   @Test public void erlangFunction_generatesCorrectJson() throws JSONException {
      ErlangFunction f = new ErlangFunction("testing", "doit");
      JSONObject json = f.toJson();
      
      assertEquals("testing", json.get("module"));
      assertEquals("erlang", json.get("language"));
      assertEquals("doit", json.get("function"));
      //assertEquals("{\"module\":\"testing\",\"language\":\"erlang\",\"function\":\"doit\"}", json);
   }
   
   @Test public void namedJSFunction_generatesCorrectJson() throws JSONException {
      JavascriptFunction f = JavascriptFunction.named("Riak.mapValuesJson");
      String json = f.toJson().toString();
      assertEquals("{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\"}", json);
   }

   @Test public void anonJSFunction_generatesCorrectJson() throws JSONException {
      JavascriptFunction f = JavascriptFunction.anon("function(v) { return [v]; }");
      String json = f.toJson().toString();
      assertEquals("{\"source\":\"function(v) { return [v]; }\",\"language\":\"javascript\"}", json);
   }
   
   @Test public void anonJSFunction_escapesQuotes() throws JSONException {
      JavascriptFunction f = JavascriptFunction.anon("function(v) { return [{\"value\": v}]; }");
      String json = f.toJson().toString();
      assertEquals("{\"source\":\"function(v) { return [{\\\"value\\\": v}]; }\",\"language\":\"javascript\"}", json);
   }

}
