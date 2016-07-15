/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.MultiFetch;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestMultiFetch extends ITestAutoCleanupBase
{

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        String keyPrefix = "key_";
        String valuePrefix = "value_";
        List<Location> locations = new LinkedList<>();
        List<BinaryValue> values = new LinkedList<>();
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        // Store some stuff
        for (int i = 0; i < 5; i++)
        {
            String key = keyPrefix + i;
            String value = valuePrefix + i;
            Location loc = new Location(ns, key);
            locations.add(loc);
            values.add(BinaryValue.create(value));
            RiakObject o = new RiakObject().setValue(BinaryValue.create(value));

            StoreValue sv = new StoreValue.Builder(o).withLocation(loc).build();
            client.execute(sv);
        }

        MultiFetch mf = new MultiFetch.Builder().addLocations(locations).build();

        MultiFetch.Response mfr = client.execute(mf);

        assertEquals(locations.size(), mfr.getResponses().size());

        List<Location> returnedLocations = new LinkedList<>();
        List<BinaryValue> returnedValues = new LinkedList<>();

        for (RiakFuture<FetchValue.Response, Location> future : mfr.getResponses())
        {
            returnedLocations.add(future.getQueryInfo());
            returnedValues.add(future.get().getValue(RiakObject.class).getValue());
        }

        assertTrue(returnedLocations.containsAll(locations));
        assertTrue(returnedValues.containsAll(values));




    }
}
