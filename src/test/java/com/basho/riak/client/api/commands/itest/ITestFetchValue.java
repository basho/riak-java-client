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

import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.api.commands.kv.FetchValue.Option;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.DefaultResolver;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.convert.JSONConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.*;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Assume;
import org.junit.Before;
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
    public void simpleTestDefaultType()
    {
        simpleTest(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void simpleTestTestType()
    {
        Assume.assumeTrue(testBucketType);
        simpleTest(bucketType.toString());
    }

    private void simpleTest(String bucketType)
    {
        try
        {
            Namespace ns = new Namespace(bucketType, bucketName.toString());
            Location loc = new Location(ns, "test_fetch_key1");

            Pojo pojo = new Pojo();
            pojo.value = "test value";
            StoreValue sv =
                new StoreValue.Builder(pojo).withLocation(loc).build();

            StoreValue.Response resp = client.execute(sv);


            FetchValue fv = new FetchValue.Builder(loc).build();
            FetchValue.Response fResp = client.execute(fv);

            assertEquals(pojo.value, fResp.getValue(Pojo.class).value);

            RiakObject ro = fResp.getValue(RiakObject.class);
            assertNotNull(ro.getValue());
            assertEquals("{\"value\":\"test value\"}", ro.getValue().toString());
        }
        catch (ExecutionException ex)
        {
            System.out.println(ex.getCause().getCause());
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(ITestFetchValue.class.getName()).log(Level.SEVERE, null, ex);
        }

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
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        Location loc = new Location(ns, "test_fetch_key2");
        FetchValue fv = new FetchValue.Builder(loc).build();
        FetchValue.Response fResp = client.execute(fv);

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
                            .withOption(Option.DELETED_VCLOCK, false)
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

        FetchValue fv = new FetchValue.Builder(loc).build();

        FetchValue.Response fResp = client.execute(fv);

        assertEquals(2, fResp.getNumberOfValues());
        assertNotNull(fResp.getVectorClock());

        assertEquals(pojo.value, fResp.getValue(Pojo.class).value);

        JSONConverter<Pojo> converter = new JSONConverter<Pojo>(Pojo.class);

        assertEquals(pojo.value, fResp.getValues(converter).get(0).value);
        assertEquals(pojo.value, fResp.getValue(converter, resolver).value);
        assertEquals(pojo.value, fResp.getValue(pojoTypeRef).value);
    }

    private Pojo storeSiblings(Location loc) throws ExecutionException, InterruptedException
    {
        Pojo pojo = new Pojo();
        pojo.value = "test value";
        StoreValue sv =
            new StoreValue.Builder(pojo).withLocation(loc).build();

        client.execute(sv);

        pojo.value = "Pick me!";

        sv = new StoreValue.Builder(pojo).withLocation(loc).build();

        client.execute(sv);
        return pojo;
    }

    @Test
    public void fetchAnnotatedPojoDefaultType() throws ExecutionException, InterruptedException
    {
        fetchAnnotatedPojo(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void fetchAnnotatedPojoTestType() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testBucketType);
        fetchAnnotatedPojo(bucketType.toString());
    }

    private void fetchAnnotatedPojo(String bucketType) throws ExecutionException, InterruptedException
    {
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        Location loc = new Location(ns, "test_fetch_key5");

        String jsonValue = "{\"value\":\"my value\"}";

        RiakObject ro = new RiakObject()
                        .setValue(BinaryValue.create(jsonValue))
                        .setContentType("application/json");

        StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
        client.execute(sv);

        FetchValue fv = new FetchValue.Builder(loc).build();
        FetchValue.Response resp = client.execute(fv);

        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);

        assertNotNull(rap.bucketName);
        assertEquals(ns.getBucketNameAsString(), rap.bucketName);
        assertNotNull(rap.key);
        assertEquals(loc.getKeyAsString(), rap.key);
        assertNotNull(rap.bucketType);
        assertEquals(ns.getBucketTypeAsString(), rap.bucketType);
        assertNotNull(rap.contentType);
        assertEquals(ro.getContentType(), rap.contentType);
        assertNotNull(rap.vclock);
        assertNotNull(rap.vtag);
        assertNotNull(rap.lastModified);
        assertNotNull(rap.value);
        assertFalse(rap.deleted);
        assertNotNull(rap.value);
        assertEquals("my value", rap.value);
    }

    @Test
    public void fetchAnnotatedPojoWIthIndexes() throws ExecutionException, InterruptedException
    {
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns,"test_fetch_key6");

        String jsonValue = "{\"value\":\"my value\"}";

        RiakObject ro = new RiakObject()
                        .setValue(BinaryValue.create(jsonValue))
                        .setContentType("application/json");

        ro.getIndexes().getIndex(StringBinIndex.named("email")).add("roach@basho.com");
        ro.getIndexes().getIndex(LongIntIndex.named("user_id")).add(1L);

        StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
        client.execute(sv);

        FetchValue fv = new FetchValue.Builder(loc).build();
        FetchValue.Response resp = client.execute(fv);

        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);

        assertNotNull(rap.emailIndx);
        assertTrue(rap.emailIndx.contains("roach@basho.com"));
        assertEquals(rap.userId.longValue(), 1L);
    }

    public static class Pojo
    {
        @JsonProperty
        String value;

        @RiakVClock
        VClock vclock;

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
