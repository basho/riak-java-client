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

import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.api.commands.kv.UpdateValue.Update;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestUpdateValue extends ITestAutoCleanupBase
{
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_update_key1");
        UpdateValue uv = new UpdateValue.Builder(loc)
                            .withStoreOption(Option.RETURN_BODY, true)
                            .withUpdate(new UpdatePojo())
                            .build();

        UpdateValue.Response resp = client.execute(uv);
        Pojo pojo = resp.getValue(Pojo.class);
        assertEquals(1, pojo.value);

        resp = client.execute(uv);
        pojo = resp.getValue(Pojo.class);

        assertEquals(2, pojo.value);
    }

    @Test
    public void updateAnnotatedPojo() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_update_key2");
        UpdateValue uv = new UpdateValue.Builder(loc)
                            .withStoreOption(Option.RETURN_BODY, true)
                            .withUpdate(new UpdateAnnotatedPojo())
                            .build();

        UpdateValue.Response resp = client.execute(uv);
        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);

        assertNotNull(rap.bucketName);
        assertEquals(ns.getBucketNameAsString(), rap.bucketName);
        assertNotNull(rap.key);
        assertEquals(loc.getKeyAsString(), rap.key);
        assertNotNull(rap.bucketType);
        assertEquals(ns.getBucketTypeAsString(), rap.bucketType);
        assertNotNull(rap.contentType);
        assertEquals("application/json", rap.contentType);
        assertNotNull(rap.vclock);
        assertNotNull(rap.vtag);
        assertNotNull(rap.lastModified);
        assertNotNull(rap.value);
        assertFalse(rap.deleted);
        assertNotNull(rap.value);
        assertEquals("updated value", rap.value);
    }

    public static class UpdatePojo extends Update<Pojo>
    {
        @Override
        public Pojo apply(Pojo original)
        {
            if (original == null)
            {
                original = new Pojo();
            }

            original.value++;
            return original;
        }
    }

    public static class Pojo
    {
        @JsonProperty
        int value;

        @RiakVClock
        VClock vclock;
    }

    public static class UpdateAnnotatedPojo extends Update<RiakAnnotatedPojo>
    {
        @Override
        public RiakAnnotatedPojo apply(RiakAnnotatedPojo original)
        {
            if (original == null)
            {
                original = new RiakAnnotatedPojo();
                original.value = "updated value";
            }
            return original;
        }
    }
}
