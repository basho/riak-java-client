/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.operations.itest;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.operations.mapreduce.BucketKeyMapReduce;
import com.basho.riak.client.operations.mapreduce.MapReduce;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestMapReduce extends ITestBase
{

	RiakClient client = new RiakClient(cluster);

	@Test
	public void testBasicMR() throws InterruptedException, ExecutionException, IOException
	{
		RiakObject obj = new RiakObject();

		obj.setValue(BinaryValue.create("Alice was beginning to get very tired of sitting by her sister on the " +
				"bank, and of having nothing to do: once or twice she had peeped into the " +
				"book her sister was reading, but it had no pictures or conversations in " +
				"it, 'and what is the use of a book,' thought Alice 'without pictures or " +
				"conversation?'"));
		Location location = new Location(bucketName).setKey("p1");
		client.execute(new StoreValue.Builder(obj).withLocation(location).build());

		obj.setValue(BinaryValue.create("So she was considering in her own mind (as well as she could, for the " +
				"hot day made her feel very sleepy and stupid), whether the pleasure " +
				"of making a daisy-chain would be worth the trouble of getting up and " +
				"picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
				"close by her."));

		location = new Location(bucketName).setKey("p2");
		client.execute(new StoreValue.Builder(obj).withLocation(location).build());


		obj.setValue(BinaryValue.create("The rabbit-hole went straight on like a tunnel for some way, and then " +
				"dipped suddenly down, so suddenly that Alice had not a moment to think " +
				"about stopping herself before she found herself falling down a very deep " +
				"well."));
		location = new Location(bucketName).setKey("p3");
		client.execute(new StoreValue.Builder(obj).withLocation(location).build());

		MapReduce mr = new BucketKeyMapReduce.Builder()
				.withLocation(new Location(bucketName).setKey("p1"))
				.withLocation(new Location(bucketName).setKey("p2"))
				.withLocation(new Location(bucketName).setKey("p3"))
				.withMapPhase(Function.newAnonymousJsFunction(
						"function(v, key_data) {" +
								"  var m = v.values[0].data.toLowerCase().match(/\\w*/g);" +
								"  var r = [];" +
								"  for(var i in m) {" +
								"    if(m[i] != '') {" +
								"      var o = {};" +
								"      o[m[i]] = 1;" +
								"      r.push(o);" +
								"     }" +
								"  }" +
								"  return r;" +
								"}"
				))
				.withReducePhase(Function.newAnonymousJsFunction(
						"function(v, key_data) {" +
								"  var r = {};" +
								"  for(var i in v) {" +
								"    for(var w in v[i]) {" +
								"      if(w in r)" +
								"        r[w] += v[i][w];" +
								"      else" +
								"        r[w] = v[i][w];" +
								"     }" +
								"  }" +
								"  return [r];" +
								"}"
				), true)
				.build();

		MapReduce.Response response = client.execute(mr);
		List<BinaryValue> resultList = response.getOutput();

		// The query should return one result which is a JSON array containing a
		// single JSON object that is a set of word counts.
		assertEquals(1, resultList.size());

		String json = resultList.get(0).toString();
		ObjectMapper oMapper = new ObjectMapper();
		@SuppressWarnings("unchecked")
		List<Map<String, Integer>> jsonList = oMapper.readValue(json, List.class);
		Map<String, Integer> resultMap = jsonList.get(0);

		assertNotNull(resultMap.containsKey("the"));
		assertEquals(Integer.valueOf(8), resultMap.get("the"));

	}
}
