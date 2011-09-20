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

import com.basho.riak.pbc.RiakObject;
import com.basho.riak.test.util.JSONEquals;

public class TestMapReduceBuilder {

   @Test public void canStoreBucket() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("foo");
      assertEquals(builder.getBucket(), "foo");
   }

   @Test public void canStoreSearch() {
       MapReduceBuilder builder = new MapReduceBuilder();
       builder.setSearch("foo:bar");
       assertEquals(builder.getSearch(), "foo:bar");
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
       List<RiakObject> riakObjects = Arrays.asList(new RiakObject[] { new RiakObject("foo", "bar", "v"),
                                                                   new RiakObject("foo", "baz", "v")});
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

   @Test(expected=IllegalStateException.class)
   public void canUseOnlyBucket_collection() {
      MapReduceBuilder builder = new MapReduceBuilder();
      builder.setBucket("wubba");
      List<RiakObject> riakObjects = Arrays.asList(new RiakObject[] { new RiakObject("foo", "bar", "v"),
                                                                      new RiakObject("foo", "baz", "v")});
      builder.setRiakObjects(riakObjects);
   }

   @Test(expected=IllegalStateException.class)
   public void cannotUseObjectsAndSearch() {
       MapReduceBuilder builder = new MapReduceBuilder();
       builder.addRiakObject("foo", "bar");
       builder.setSearch("foo:bar");
   }

   @Test public void canUseBucketAndSearch() {
       MapReduceBuilder builder = new MapReduceBuilder();
       builder.setBucket("foo");
       builder.setSearch("foo:bar");
       assertEquals(builder.getBucket(), "foo");
       assertEquals(builder.getSearch(), "foo:bar");
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

    @Test public void canBuildJSMapOnlyJobWithSearch() throws JSONException {
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket("gumby");
        builder.setSearch("horse:pokey");
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), true);

        JSONObject expected = new JSONObject(
                                             "{\"inputs\":{\"module\":\"riak_search\",\"function\":\"mapred_search\","
                                                     + "\"arg\":[\"gumby\",\"horse:pokey\"]},\"query\":[{\"map\":{\"name\":\"Riak.mapValuesJson\","
                                                     + "\"language\":\"javascript\",\"keep\":true}}]}");

        assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
    }

    @Test public void canBuildJSMapReduceJobWithSearch() throws JSONException {
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket("gumby");
        builder.setSearch("horse:pokey");
        builder.map(JavascriptFunction.anon("function(v) { return [v]; }"), false);
        builder.reduce(JavascriptFunction.named("Riak.reduceMin"), true);

        JSONObject expected = new JSONObject(
                                             "{\"inputs\":{\"module\":\"riak_search\",\"function\":\"mapred_search\","
                                                     + "\"arg\":[\"gumby\",\"horse:pokey\"]},\"query\":[{\"map\":{\"source\":"
                                                     + "\"function(v) { return [v]; }\",\"language\":\"javascript\",\"keep\":false}},"
                                                     + "{\"reduce\":{\"name\":\"Riak.reduceMin\",\"language\":\"javascript\",\"keep\":true}}]}");

        assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
    }

    @Test public void canBuildErlangMapOnlyJobWithSearch() throws JSONException {
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket("gumby");
        builder.setSearch("horse:pokey");
        builder.map(new ErlangFunction("foo", "bar"), true);

        JSONObject expected = new JSONObject(
                                             "{\"inputs\":{\"module\":\"riak_search\",\"function\":\"mapred_search\","
                                                     + "\"arg\":[\"gumby\",\"horse:pokey\"]},\"query\":[{\"map\":{\"module\":\"foo\","
                                                     + "\"language\":\"erlang\",\"keep\":true,\"function\":\"bar\"}}]}");

        assertTrue("Generated JSON not as expected", JSONEquals.equals(expected, builder.toJSON()));
    }

    @Test public void canBuildErlangMapReduceJobWithSearch() throws JSONException {
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket("gumby");
        builder.setSearch("horse:pokey");
        builder.map(new ErlangFunction("foo", "bar"), false);
        builder.reduce(new ErlangFunction("baz", "quux"), true);

        JSONObject expected = new JSONObject(
                                             "{\"inputs\":{\"module\":\"riak_search\",\"function\":\"mapred_search\","
                                                     + "\"arg\":[\"gumby\",\"horse:pokey\"]},\"query\":[{\"map\":{\"module\":\"foo\","
                                                     + "\"language\":\"erlang\",\"keep\":false,\"function\":\"bar\"}},"
                                                     + "{\"reduce\":{\"module\":\"baz\",\"language\":\"erlang\",\"keep\":true,\"function\":\"quux\"}}]}");

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
}