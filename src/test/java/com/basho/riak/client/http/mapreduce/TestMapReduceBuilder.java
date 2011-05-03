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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.mapreduce.ErlangFunction;
import com.basho.riak.client.http.mapreduce.JavascriptFunction;
import com.basho.riak.client.http.mapreduce.filter.BetweenFilter;
import com.basho.riak.client.http.mapreduce.filter.EndsWithFilter;
import com.basho.riak.client.http.mapreduce.filter.EqualToFilter;
import com.basho.riak.client.http.mapreduce.filter.FloatToStringFilter;
import com.basho.riak.client.http.mapreduce.filter.GreaterThanFilter;
import com.basho.riak.client.http.mapreduce.filter.GreaterThanOrEqualFilter;
import com.basho.riak.client.http.mapreduce.filter.IntToStringFilter;
import com.basho.riak.client.http.mapreduce.filter.LessThanFilter;
import com.basho.riak.client.http.mapreduce.filter.LessThanOrEqualFilter;
import com.basho.riak.client.http.mapreduce.filter.LogicalAndFilter;
import com.basho.riak.client.http.mapreduce.filter.LogicalFilterGroup;
import com.basho.riak.client.http.mapreduce.filter.LogicalNotFilter;
import com.basho.riak.client.http.mapreduce.filter.LogicalOrFilter;
import com.basho.riak.client.http.mapreduce.filter.MatchFilter;
import com.basho.riak.client.http.mapreduce.filter.NotEqualToFilter;
import com.basho.riak.client.http.mapreduce.filter.SetMemberFilter;
import com.basho.riak.client.http.mapreduce.filter.SimilarToFilter;
import com.basho.riak.client.http.mapreduce.filter.StartsWithFilter;
import com.basho.riak.client.http.mapreduce.filter.StringToFloatFilter;
import com.basho.riak.client.http.mapreduce.filter.StringToIntFilter;
import com.basho.riak.client.http.mapreduce.filter.ToLowerFilter;
import com.basho.riak.client.http.mapreduce.filter.ToUpperFilter;
import com.basho.riak.client.http.mapreduce.filter.TokenizeFilter;
import com.basho.riak.client.http.mapreduce.filter.UrlDecodeFilter;
import com.basho.riak.client.http.request.MapReduceBuilder;
import com.basho.riak.test.util.JSONEquals;

public class TestMapReduceBuilder {

   @Test public void canStoreBucket() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("foo");
      assertEquals(builder.getBucket(), "foo");
   }
   
   @Test public void canStoreObjects() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("foo", "bar");
      Map<String, Set<String>> objects = builder.getRiakObjects();
      assertEquals(objects.get("foo").size(), 1);
      assertTrue(objects.get("foo").contains("bar"));
      // Verify duplicates are not added
      builder.addRiakObject("foo", "bar");
      assertEquals(1, builder.getRiakObjects().get("foo").size());
   }
   
   @Test public void canRemoveObjects() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("foo", "bar");
      builder.addRiakObject("foo", "baz");
      assertEquals(2, builder.getRiakObjects().get("foo").size());
      builder.removeRiakObject("foo", "bar");
      assertEquals(1, builder.getRiakObjects().get("foo").size());
   }
   
   @Test public void nullParamClearsObject() {
       MapReduceBuilder builder = new MapReduceBuilder();
       builder.addRiakObject("foo", "bar");
       builder.setRiakObjects((Map<String, Set<String>>) null);
       assertEquals(0, builder.getRiakObjects().size());

       builder.addRiakObject("foo", "bar");
       builder.setRiakObjects((Collection<RiakObject>) null);
       assertEquals(0, builder.getRiakObjects().size());
   }
   
   @Test public void extractsRiakObjectInfo() {
       MapReduceBuilder builder = new MapReduceBuilder();
       List<RiakObject> riakObjects = Arrays.asList(new RiakObject[] { new RiakObject("foo", "bar"), 
                                                                   new RiakObject("foo", "baz")});
       builder.setRiakObjects(riakObjects);

       Map<String, Set<String>> objects = builder.getRiakObjects();
       assertEquals(objects.get("foo").size(), 2);
       assertTrue(objects.get("foo").contains("bar"));
       assertTrue(objects.get("foo").contains("baz"));
   }
   
   @Test(expected=IllegalStateException.class)
   public void canUseOnlyObjects() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("foo", "bar");
      builder.setBucket("wubba");
   }
   
   @Test(expected=IllegalStateException.class)
   public void canUseOnlyBucket() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.addRiakObject("foo", "bar");
   }
   
   @Test public void canBuildJSMapOnlyJobWithBucket() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":" + 
                                           "[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}}]}");
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
      
   }
   
   @Test public void canBuildJSMapReduceJobWithBucket() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.anon("function(v) { return [v]; }"), false);
      builder.reduce(JavascriptFunction.named("Riak.reduceMin"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"source\":" +
                                           "\"function(v) { return [v]; }\",\"language\":\"javascript\",\"keep\":false}}," + 
                                           "{\"reduce\":{\"name\":\"Riak.reduceMin\",\"language\":\"javascript\",\"keep\":true}}]}");
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   
   @Test public void canBuildErlangMapOnlyJobWithBucket() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(new ErlangFunction("foo", "bar"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"module\":\"foo\"," +
                                           "\"language\":\"erlang\",\"keep\":true,\"function\":\"bar\"}}]}");
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildErlangMapReduceJobWithBucket() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(new ErlangFunction("foo", "bar"), false);
      builder.reduce(new ErlangFunction("baz", "quux"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"module\":\"foo\"," +
                                           "\"language\":\"erlang\",\"keep\":false,\"function\":\"bar\"}}," +
                                           "{\"reduce\":{\"module\":\"baz\",\"language\":\"erlang\",\"keep\":true,\"function\":\"quux\"}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   @Test public void canBuildJSMapOnlyJobWithObjects() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("first", "key1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), true);
      
      
      JSONObject expected = new JSONObject();
      JSONArray inputs = new JSONArray();
      inputs.put(new String[] {"first", "key1"});
      expected.put("inputs", inputs);
      JSONArray query = new JSONArray("[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}}]");     
      expected.put("query", query);
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildJSMapReduceJobWithObjects() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("first", "key2");
      builder.map(JavascriptFunction.anon("function(v) { return [v]; }"), false);
      builder.reduce(JavascriptFunction.named("Riak.reduceMin"), true);
      
      
      JSONObject expected = new JSONObject();
      JSONArray inputs = new JSONArray();
      inputs.put(new String[] {"first", "key2"});
      expected.put("inputs", inputs);
      JSONArray query = new JSONArray("[{\"map\":{\"source\":" +
                                      "\"function(v) { return [v]; }\",\"language\":\"javascript\",\"keep\":false}}," + 
                                      "{\"reduce\":{\"name\":\"Riak.reduceMin\",\"language\":\"javascript\",\"keep\":true}}]");     
      expected.put("query", query);
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   
   @Test public void canBuildErlangMapOnlyJobWithObjects() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.addRiakObject("first", "key1");
      builder.addRiakObject("second", "key1");
      builder.map(new ErlangFunction("foo", "bar"), true);
      
      JSONObject expected = new JSONObject();
      JSONArray inputs = new JSONArray();
      inputs.put(new String[] {"first", "key1"});
      inputs.put(new String[] {"second", "key1"});
      expected.put("inputs", inputs);
      JSONArray query = new JSONArray("[{\"map\":{\"module\":\"foo\",\"language\":\"erlang\",\"keep\":true,\"function\":\"bar\"}}]");     
      expected.put("query", query);
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildErlangMapReduceJobWitObjects() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(new ErlangFunction("foo", "bar"), false);
      builder.reduce(new ErlangFunction("baz", "quux"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"module\":\"foo\"," +
                                           "\"language\":\"erlang\",\"keep\":false,\"function\":\"bar\"}}," +
                                           "{\"reduce\":{\"module\":\"baz\",\"language\":\"erlang\",\"keep\":true,\"function\":\"quux\"}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildLinkMapReduceJob() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.link("foo", false);
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"link\":{\"bucket\":\"foo\",\"keep\":false}}," +
                                           "{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @SuppressWarnings("unchecked") @Test public void canBuildMapJobWithPhaseArg() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), "123", true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"arg\":\"123\"," +
                                           "\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
      
      builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), 123, true);
      
      expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"arg\":123," +
                                "\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
      
      builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      List<Object> args = new ArrayList<Object>(2);
      args.add(123);
      args.add("testing");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), args, true);
      
      expected = new JSONObject("{\"inputs\":\"wubba\"}");
      
      JSONObject mapSpec = new JSONObject("{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":true}");
      mapSpec.put("arg", (Object)Arrays.asList(123, "testing"));
      JSONObject map = new JSONObject();
      map.put("map", mapSpec);
      JSONArray query = new JSONArray();
      query.put(map);
     
      expected.put("query", query);
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   @SuppressWarnings("unchecked") @Test public void canBuildMapReduceJobWithPhaseArg() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.reduce(JavascriptFunction.named("Riak.reduceSort"), "testing", true);
      
      JSONObject expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\"," +
                                           "\"language\":\"javascript\",\"keep\":false}},{\"reduce\":{\"arg\":\"testing\"," +
                                           "\"name\":\"Riak.reduceSort\",\"language\":\"javascript\",\"keep\":true}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
      
      builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.reduce(JavascriptFunction.named("Riak.reduceSort"), 123, true);
      
      expected = new JSONObject("{\"inputs\":\"wubba\",\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\"," +
                                           "\"language\":\"javascript\",\"keep\":false}},{\"reduce\":{\"arg\":123," +
                                           "\"name\":\"Riak.reduceSort\",\"language\":\"javascript\",\"keep\":true}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
      
      
      List<Object> args = new ArrayList<Object>(2);
      args.add(123);
      args.add("testing");
      builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), args, true);
      
      expected = new JSONObject("{\"inputs\":\"wubba\"}");
      JSONObject map = new JSONObject("{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}");
      
      JSONObject reduceSpec = new JSONObject("{\"module\":\"riak_kv_mapreduce\",\"language\":\"erlang\",\"keep\":true,\"function\":\"reduce_sort\"}");
      reduceSpec.put("arg", (Object)Arrays.asList(123, "testing"));
      JSONObject reduce = new JSONObject();
      reduce.put("reduce", reduceSpec);
      
      JSONArray query = new JSONArray();
      query.put(map);
      query.put(reduce);
      expected.put("query", query);
      
      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   /*
    * This test should always include all available key filters to ensure they render correctly.  This also
    *  implicitly verifies overall key_filter construction.
   */
   @Test public void canBuildMapJobWithKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      List<String> setMembers = new ArrayList<String>();
      setMembers.add("member1");
      setMembers.add("member2");
      setMembers.add("member3");
      builder.keyFilter(new BetweenFilter(22, 93))
             .keyFilter(new EndsWithFilter("apples"))
             .keyFilter(new EqualToFilter(36))
             .keyFilter(new FloatToStringFilter())
             .keyFilter(new GreaterThanFilter(72))
             .keyFilter(new GreaterThanOrEqualFilter(72))
             .keyFilter(new IntToStringFilter())
             .keyFilter(new LessThanFilter(42))
             .keyFilter(new LessThanOrEqualFilter(43))
             .keyFilter(new MatchFilter("fun.*"))
             .keyFilter(new NotEqualToFilter(90))
             .keyFilter(new SetMemberFilter(setMembers))
             .keyFilter(new SimilarToFilter("valeu", 2))
             .keyFilter(new StartsWithFilter("super"))
             .keyFilter(new StringToFloatFilter())
             .keyFilter(new StringToIntFilter())
             .keyFilter(new TokenizeFilter("-", 3))
             .keyFilter(new ToLowerFilter())
             .keyFilter(new ToUpperFilter())
             .keyFilter(new UrlDecodeFilter());
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"between\",22,93],[\"ends_with\",\"apples\"],[\"eq\",36],[\"float_to_string\"],[\"greater_than\",72],[\"greater_than_eq\",72],[\"int_to_string\"],[\"less_than\",42],[\"less_than_eq\",43],[\"matches\",\"fun.*\"],[\"neq\",90],[\"set_member\",\"member1\",\"member2\",\"member3\"],[\"similar_to\",\"valeu\",2],[\"starts_with\",\"super\"],[\"string_to_float\"],[\"string_to_int\"],[\"tokenize\",\"-\",3],[\"to_lower\"],[\"to_upper\"],[\"urldecode\"]],\"bucket\":\"wubba\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   /*
    * Test logical and, or, not
   */

   @Test public void canBuildMapJobWithBuilderLogicalOrKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter())
             .keyFilter(new LogicalOrFilter().add(new BetweenFilter("apples", "bananas"))
                                             .add(new GreaterThanFilter(36)))
             .keyFilter(new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"or\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildMapJobWithVarArgLogicalOrKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter())
             .keyFilter(new LogicalOrFilter(
                new BetweenFilter("apples", "bananas"), new GreaterThanFilter(36)))
             .keyFilter(new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"or\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   @Test public void canBuildMapJobWithBuilderLogicalAndKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter())
             .keyFilter(new LogicalAndFilter().add(new BetweenFilter("apples", "bananas"))
                                              .add(new GreaterThanFilter(36)))
             .keyFilter(new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"and\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   @Test public void canBuildMapJobWithVarArgLogicalAndKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter())
             .keyFilter(new LogicalAndFilter(
                new BetweenFilter("apples", "bananas"), new GreaterThanFilter(36)))
             .keyFilter(new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"and\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   @Test public void canBuildMapJobWithBuilderLogicalNotKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter())
             .keyFilter(new LogicalNotFilter().add(new BetweenFilter("apples", "bananas"))
                                              .add(new GreaterThanFilter(36)))
             .keyFilter(new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"not\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
   /*
    * Test embedded LogicalFilterGroup (var args variation)
   */
   
   @Test public void canBuildMapJobWithVarArgLogicalNotKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new IntToStringFilter(),
                        new LogicalNotFilter(new BetweenFilter("apples", "bananas"),
                                             new GreaterThanFilter(36)),
                        new BetweenFilter(12.5, 36.8));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"int_to_string\"],[\"not\",[\"between\",\"apples\",\"bananas\"],[\"greater_than\",36]],[\"between\",12.5,36.8]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }

   /*
    * Test nested logical and, or, not
   */
   
   @Test public void canBuildMapJobWithCompoundLogicalKeyFilters() throws JSONException {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba1");
      builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
      builder.keyFilter(new LogicalOrFilter(
        new LogicalOrFilter(
            new LogicalFilterGroup(
                new StringToIntFilter(),
                new GreaterThanOrEqualFilter(98)),
            new LogicalFilterGroup(
                new StringToIntFilter(),
                new LessThanFilter(2))),
        new LogicalAndFilter(
            new LogicalFilterGroup(
                new StringToIntFilter(),
                new BetweenFilter(18, 36)),
            new LogicalFilterGroup(
                new StringToIntFilter(),
                new GreaterThanFilter(0))),
        new LogicalNotFilter(
            new LogicalFilterGroup(
                new StringToIntFilter(),
                new EqualToFilter(12)))));
      
      JSONObject expected = new JSONObject("{\"inputs\":{\"key_filters\":[[\"or\",[\"or\",[[\"string_to_int\"],[\"greater_than_eq\",98]],[[\"string_to_int\"],[\"less_than\",2]]],[\"and\",[[\"string_to_int\"],[\"between\",18,36]],[[\"string_to_int\"],[\"greater_than\",0]]],[\"not\",[[\"string_to_int\"],[\"eq\",12]]]]],\"bucket\":\"wubba1\"},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\",\"language\":\"javascript\",\"keep\":false}}]}");

      assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
   }
   
}