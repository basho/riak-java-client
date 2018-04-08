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

import com.basho.riak.client.RiakTestFunctions;
import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.convert.JSONConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;

import com.fasterxml.jackson.core.type.TypeReference;
import static net.javacrumbs.jsonunit.JsonAssert.*;
import net.javacrumbs.jsonunit.core.Option;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestFetchValue extends ITestAutoCleanupBase
{
    private RiakClient client = new RiakClient(cluster);

    @Test
    public void simpleTestDefaultType() throws ExecutionException, InterruptedException {
        simpleTest(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void simpleTestTestType() throws ExecutionException, InterruptedException {
        Assume.assumeTrue(testBucketType);
        simpleTest(bucketType.toString());
    }

    private void simpleTest(String bucketType) throws ExecutionException, InterruptedException {
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        Location loc = new Location(ns, "test_fetch_key1");

        Pojo pojo = new Pojo();
        pojo.value = "test value";

        createKValue(client, loc, pojo, false);

        final Pojo theFetchedPojo = fetchByLocationAs(client, loc, Pojo.class);
        assertJsonEquals("{value: 'test value', vclock: '${json-unit.regex}[a-zA-Z0-9+/=]+'}", theFetchedPojo);
    }

    @Test
    public void notFoundDefaultType() throws ExecutionException, InterruptedException
    {
        notFound(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void notFoundTestType() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testBucketType);
        notFound(bucketType.toString());
    }

    private void notFound(String bucketType) throws ExecutionException, InterruptedException
    {
        final FetchValue.Response fResp = fetchByLocation(client,
                new Location(new Namespace(bucketType, bucketName.toString()), "test_fetch_key2"));

        assertFalse(fResp.hasValues());
        assertTrue(fResp.isNotFound());
        assertNull(fResp.getValue(Pojo.class));
        RiakObject ro = fResp.getValue(RiakObject.class);
    }

    // Apparently this isn't happening anymore. or it only happens with leveldb
    // Leaving it here to investigate
    @Ignore
    @Test
    public void ReproRiakTombstoneBehavior() throws ExecutionException, InterruptedException
    {
        // We're back to allow_mult=false as default
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation op =
            new StoreBucketPropsOperation.Builder(ns)
                .withAllowMulti(true)
                .build();
        cluster.execute(op);
        op.get();

        Location loc = new Location(ns, "test_fetch_key3");

        Pojo pojo = new Pojo();
        pojo.value = "test value";
        StoreValue sv =
            new StoreValue.Builder(pojo).withLocation(loc).build();

        client.execute(sv);

        resetAndEmptyBucket(bucketName);

        client.execute(sv);

        FetchValue fv = new FetchValue.Builder(loc)
                            .withOption(FetchValue.Option.DELETED_VCLOCK, false)
                            .build();

        FetchValue.Response fResp = client.execute(fv);

        assertEquals(2, fResp.getValues(RiakObject.class).size());
    }

    @Test
    public void resolveSiblingsDefaultType() throws ExecutionException, InterruptedException
    {
        ConflictResolver<Pojo> resolver = new MyResolver();
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        setAllowMultToTrue(ns);
        resolveSiblings(ns, resolver);
        resetAndEmptyBucket(ns);
    }

    private void setAllowMultToTrue(Namespace namespace) throws ExecutionException, InterruptedException
    {
        StoreBucketPropsOperation op =
                new StoreBucketPropsOperation.Builder(namespace)
                        .withAllowMulti(true)
                        .withLastWriteWins(false)
                        .build();
        cluster.execute(op);
        op.get();
    }

    @Test
    public void resolveSiblingsTestType() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testBucketType);
        ConflictResolver<Pojo> resolver = new MyResolver();

        Namespace ns = new Namespace(bucketType.toString(), bucketName.toString());

        resolveSiblings(ns, resolver);
    }

    private void resolveSiblings(Namespace ns, ConflictResolver<Pojo> resolver) throws ExecutionException, InterruptedException
    {
        Location loc = new Location(ns, "test_fetch_key4");

        Pojo pojo = storeSiblings(loc);
        TypeReference<Pojo>  pojoTypeRef = new TypeReference<Pojo>() {};

        ConflictResolverFactory.getInstance()
            .registerConflictResolver(Pojo.class, resolver);

        ConflictResolverFactory.getInstance()
            .registerConflictResolver(pojoTypeRef, resolver);

        FetchValue.Response fResp = fetchByLocation(client, loc);

        assertEquals(2, fResp.getNumberOfValues());
        assertNotNull(fResp.getVectorClock());

        assertEquals(pojo.value, fResp.getValue(Pojo.class).value);

        JSONConverter<Pojo> converter = new JSONConverter<>(Pojo.class);

        assertEquals(pojo.value, fResp.getValues(converter).get(0).value);
        assertEquals(pojo.value, fResp.getValue(converter, resolver).value);
        assertEquals(pojo.value, fResp.getValue(pojoTypeRef).value);

        assertEquals(loc, fResp.getLocation());
    }

    private Pojo storeSiblings(Location loc) throws ExecutionException, InterruptedException
    {
        Pojo pojo = new Pojo();
        pojo.value = "test value";

        createKValue(client, loc, pojo, false);

        pojo.value = "Pick me!";

        createKValue(client, loc, pojo, false);
        return pojo;
    }

    @Test
    public void fetchAnnotatedPojoDefaultType() throws ExecutionException, InterruptedException, IOException
    {
        fetchAnnotatedPojo(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void fetchAnnotatedPojoTestType() throws ExecutionException, InterruptedException, IOException
    {
        Assume.assumeTrue(testBucketType);
        fetchAnnotatedPojo(bucketType.toString());
    }

    private void fetchAnnotatedPojo(String bucketType) throws ExecutionException, InterruptedException, IOException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString());

        createKVData(client, ns, "{" +
                "   key: 'test_fetch_key5', " +
                "   value: {" +
                "       value: 'my value'" +
                "   }" +
                "}");

        Location loc = new Location(ns, "test_fetch_key5");

        final RiakAnnotatedPojo rap = fetchByLocationAs(client,
                new Location(ns, "test_fetch_key5"),
                RiakAnnotatedPojo.class);

        assertJsonEquals("{" +
                "key: 'test_fetch_key5', " +
                "bucketName: 'ITestBase'," +
                "bucketType: '" + ns.getBucketTypeAsString()  +"'," +
                "contentType: 'application/json'," +
                "deleted: false," +
                "value: 'my value'," +
                "vclock: '${json-unit.any-string}'," +
                "vtag: '${json-unit.any-string}'," +
                "lastModified: '${json-unit.any-number}'" +
            "}",
            rap,
            when(Option.IGNORING_EXTRA_FIELDS));
    }

    @Test
    public void fetchAnnotatedPojoWIthIndexes() throws ExecutionException, InterruptedException, IOException
    {
        final Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        createKVData(client, ns, "{" +
                "   key: 'test_fetch_key6', " +
                "   value: {" +
                "       value: 'my value'" +
                "   }," +
                "   indices: {" +
                "       email: 'roach@basho.com'," +
                "       user_id: 1" +
                "   }" +
                "}");

        final RiakAnnotatedPojo rap = RiakTestFunctions.fetchByLocationAs(client,
                new Location(ns,"test_fetch_key6"),
                RiakAnnotatedPojo.class);

        assertJsonEquals("{" +
                "   key: 'test_fetch_key6'," +
                "   emailIndx: [" +
                "       'roach@basho.com'" +
                "  ]," +
                "   userId: 1" +
                "}",
                rap,
                when(Option.IGNORING_EXTRA_FIELDS));
    }

    public static class Pojo
    {
        @JsonProperty
        public String value;

        @RiakVClock
        public VClock vclock;
    }

    public static class MyResolver implements ConflictResolver<Pojo>
    {
        @Override
        public Pojo resolve(List<Pojo> objectList) throws UnresolvedConflictException
        {
            if (objectList.size() > 0)
            {
                for (Pojo p : objectList)
                {
                    if (p.value.equals("Pick me!"))
                    {
                        return p;
                    }
                }

                return objectList.get(0);
            }
            return null;
        }
    }
}
