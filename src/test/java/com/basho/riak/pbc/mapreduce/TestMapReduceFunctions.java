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
package com.basho.riak.pbc.mapreduce;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.test.util.JSONEquals;

public class TestMapReduceFunctions {

   @Test public void erlangFunction_generatesCorrectJson() throws JSONException {
      ErlangFunction f = new ErlangFunction("testing", "doit");
      JSONObject json = f.toJson();

      assertEquals("testing", json.get("module"));
      assertEquals("erlang", json.get("language"));
      assertEquals("doit", json.get("function"));
   }

   @Test public void namedJSFunction_generatesCorrectJson() throws JSONException {
      JavascriptFunction f = JavascriptFunction.named("Riak.mapValuesJson");
      assertTrue(JSONEquals.equals(new JSONObject("{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\"}"), f.toJson()));
   }

   @Test public void anonJSFunction_generatesCorrectJson() throws JSONException {
      JavascriptFunction f = JavascriptFunction.anon("function(v) { return [v]; }");
      assertTrue(JSONEquals.equals(new JSONObject("{\"source\":\"function(v) { return [v]; }\",\"language\":\"javascript\"}"), f.toJson()));
   }

   @Test public void anonJSFunction_escapesQuotes() throws JSONException {
      JavascriptFunction f = JavascriptFunction.anon("function(v) { return [{\"value\": v}]; }");
      assertTrue(JSONEquals.equals(new JSONObject("{\"source\":\"function(v) { return [{\\\"value\\\": v}]; }\",\"language\":\"javascript\"}"), f.toJson()));
   }

   @Test public void linkFunctionCTORStoresArgs() throws Exception {
       LinkFunction f = new LinkFunction("bucket", "tag");
       assertTrue(JSONEquals.equals(new JSONObject("{\"tag\":\"tag\",\"bucket\":\"bucket\"}"), f.toJson()));
   }
}
