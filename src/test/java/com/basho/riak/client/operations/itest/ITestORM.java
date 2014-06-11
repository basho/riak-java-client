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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.ConflictResolverFactory;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.kv.FetchValue;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.annotations.RiakVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.RiakJacksonModule;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.operations.kv.StoreValue.Option;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.operations.kv.UpdateValue;
import com.basho.riak.client.operations.kv.UpdateValue.Update;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestORM extends ITestBase
{
    @Before
    public void resetFactory()
    {
        TypeReference<GenericPojo<Integer>> tr = 
            new TypeReference<GenericPojo<Integer>>(){};
        ConverterFactory.getInstance().unregisterConverterForClass(tr);
        ConflictResolverFactory.getInstance().unregisterConflictResolver(tr);
    }
    
    @Test
    public void storeParamterizedTypeJSON() throws ExecutionException, InterruptedException, JsonProcessingException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key1");
        
        GenericPojo<Foo> gpf = new GenericPojo<Foo>();
        List<Foo> fooList = new ArrayList<Foo>();
        fooList.add(new Foo("Foo in list value"));
        gpf.value = new Foo("Foo in gp value");
        gpf.list = fooList;
        
        StoreValue sv = 
            new StoreValue.Builder(gpf)
                .withLocation(loc)
                .withOption(Option.RETURN_BODY, true)
                .build();
        
        StoreValue.Response resp = client.execute(sv);
        
        
        TypeReference<GenericPojo<Foo>> tr = 
            new TypeReference<GenericPojo<Foo>>(){};
        GenericPojo<Foo> gpf2 = resp.getValue(tr);
        
        assertNotNull(gpf2);
        assertNotNull(gpf2.value);
        assertEquals(gpf.value, gpf2.value);
        assertNotNull(gpf2.list);
        assertTrue(gpf.list.containsAll(gpf2.list));

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new RiakJacksonModule());
        String json = mapper.writeValueAsString(gpf2);
        RiakObject ro = resp.getValue(RiakObject.class);
        assertEquals(json, ro.getValue().toString());
        
    }
    
    @Test
    public void storeAndFetchParamterizedTypeJSON() throws ExecutionException, InterruptedException, JsonProcessingException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key2");
        
        GenericPojo<Foo> gpf = new GenericPojo<Foo>();
        List<Foo> fooList = new ArrayList<Foo>();
        fooList.add(new Foo("Foo in list value"));
        gpf.value = new Foo("Foo in gp value");
        gpf.list = fooList;
        
        StoreValue sv = 
            new StoreValue.Builder(gpf)
                .withLocation(loc)
                .build();
        
        client.execute(sv);
            
        FetchValue fv = new FetchValue.Builder(loc).build();
        FetchValue.Response resp = client.execute(fv);
        
        TypeReference<GenericPojo<Foo>> tr = 
            new TypeReference<GenericPojo<Foo>>(){};
        GenericPojo<Foo> gpf2 = resp.getValue(tr);
        
        assertNotNull(gpf2);
        assertNotNull(gpf2.value);
        assertEquals(gpf.value, gpf2.value);
        assertNotNull(gpf2.list);
        assertTrue(gpf.list.containsAll(gpf2.list));

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new RiakJacksonModule());
        String json = mapper.writeValueAsString(gpf2);
        RiakObject ro = resp.getValue(RiakObject.class);
        assertEquals(json, ro.getValue().toString());
        
    }
    
    @Test
    public void updateParameterizedTypeJSON() throws ExecutionException, InterruptedException, JsonProcessingException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key3");
        
        MyUpdate update = new MyUpdate();
        TypeReference<GenericPojo<Integer>> tr = 
            new TypeReference<GenericPojo<Integer>>(){};
        
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(update, tr)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response resp = client.execute(uv);
        
        GenericPojo<Integer> gpi = resp.getValue(tr);
        
        assertNotNull(gpi);
        assertNotNull(gpi.value);
        assertEquals(1, gpi.value.intValue());
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new RiakJacksonModule());
        String json = mapper.writeValueAsString(gpi);
        RiakObject ro = resp.getValue(RiakObject.class);
        assertEquals(json, ro.getValue().toString());
        
        resp = client.execute(uv);
        gpi = resp.getValue(tr);
        assertNotNull(gpi);
        assertNotNull(gpi.value);
        assertEquals(2, gpi.value.intValue());
    }
    
    @Test
    public void updateAndResolveParameterizedTypeJSON() throws ExecutionException, InterruptedException
    {
        // We're back to allow_mult=false as default
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation op = 
            new StoreBucketPropsOperation.Builder(ns)
                .withAllowMulti(true)
                .build();
        cluster.execute(op);
        op.get();
        
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(ns, "test_ORM_key4");
        
        MyUpdate update = new MyUpdate();
        TypeReference<GenericPojo<Integer>> tr = 
            new TypeReference<GenericPojo<Integer>>(){};
        
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(update, tr)
                        .build();
        
        client.execute(uv);
        
        // Create a sibling 
        GenericPojo<Integer> gpi = update.apply(null);
        StoreValue sv = 
            new StoreValue.Builder(gpi)
                .withLocation(loc)
                .build();
        
        client.execute(sv);
        
        ConflictResolverFactory.getInstance().registerConflictResolver(tr, new MyResolver());
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update, tr)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response uvResp = client.execute(uv);
        
        gpi = uvResp.getValue(tr);
        assertNotNull(gpi);
        assertNotNull(gpi.value);
        assertEquals(3, gpi.value.intValue());
        
        
    }
    
    @Test
    public void updateAndResolveParameterizedTypeCustom() throws ExecutionException, InterruptedException
    {
        // We're back to allow_mult=false as default
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation op = 
            new StoreBucketPropsOperation.Builder(ns)
                .withAllowMulti(true)
                .build();
        cluster.execute(op);
        op.get();
        
        
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(ns, "test_ORM_key5");
        
        TypeReference<GenericPojo<Integer>> tr = 
            new TypeReference<GenericPojo<Integer>>(){};
        
        ConflictResolverFactory.getInstance().registerConflictResolver(tr, new MyResolver());
        ConverterFactory.getInstance().registerConverterForClass(tr, new MyConverter(tr.getType()));
        
        
        MyUpdate update = new MyUpdate();
        
        
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(update, tr)
                        .build();
        
        client.execute(uv);
        
        // Create a sibling 
        GenericPojo<Integer> gpi = update.apply(null);
        StoreValue sv = 
            new StoreValue.Builder(gpi, tr)
                .withLocation(loc)
                .build();
        
        client.execute(sv);
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update, tr)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response uvResp = client.execute(uv);
        
        gpi = uvResp.getValue(tr);
        assertNotNull(gpi);
        assertNotNull(gpi.value);
        assertEquals(3, gpi.value.intValue());
        
        // Check to see that the custom conversion is right
        RiakObject ro = uvResp.getValue(RiakObject.class);
        assertEquals("3", ro.getValue().toString());
    }
    
    
    @Test
    public void updateAndResolveRawTypeJSON() throws ExecutionException, InterruptedException, JsonProcessingException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key6");
        ConflictResolverFactory.getInstance().registerConflictResolver(Foo.class, new MyFooResolver());
        MyFooUpdate update = new MyFooUpdate();
        
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .build();
        
        client.execute(uv);
        
        // Create a sibling 
        Foo f = update.apply(null);
        StoreValue sv = 
            new StoreValue.Builder(f)
                .withLocation(loc)
                .build();
        
        client.execute(sv);
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response uvResp = client.execute(uv);
        
        f = uvResp.getValue(Foo.class);
        assertNotNull(f);
        assertEquals("Little bunny", f.fooValue);
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        uvResp = client.execute(uv);
        
        f = uvResp.getValue(Foo.class);
        assertNotNull(f);
        assertEquals("Little bunny foo foo.", f.fooValue);
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new RiakJacksonModule());
        String json = mapper.writeValueAsString(f);
        RiakObject ro = uvResp.getValue(RiakObject.class);
        assertEquals(json, ro.getValue().toString());
        
    }
    
    @Test
    public void updateAndResolveRawTypeCustom() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key7");
        
        ConflictResolverFactory.getInstance().registerConflictResolver(Foo.class, new MyFooResolver());
        ConverterFactory.getInstance().registerConverterForClass(Foo.class, new MyFooConverter());
        MyFooUpdate update = new MyFooUpdate();
        
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .build();
        
        client.execute(uv);
        
        // Create a sibling 
        Foo f = update.apply(null);
        StoreValue sv = 
            new StoreValue.Builder(f)
                .withLocation(loc)
                .build();
        
        client.execute(sv);
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response uvResp = client.execute(uv);
        
        f = uvResp.getValue(Foo.class);
        assertNotNull(f);
        assertEquals("Little bunny", f.fooValue);
        
        uv = new UpdateValue.Builder(loc)
                        .withUpdate(update)
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        uvResp = client.execute(uv);
        
        f = uvResp.getValue(Foo.class);
        assertNotNull(f);
        assertEquals("Little bunny foo foo.", f.fooValue);
        
        RiakObject ro = uvResp.getValue(RiakObject.class);
        assertEquals("Little bunny foo foo.", ro.getValue().toString());
    }
    
    @Test
    public void updateRiakObject() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        Location loc = new Location(ns, "test_ORM_key9");
        UpdateValue uv = new UpdateValue.Builder(loc)
                        .withUpdate(new Update<RiakObject>(){

                                        @Override
                                        public RiakObject apply(RiakObject original)
                                        {
                                            return new RiakObject().setValue(BinaryValue.create("value"));
                                        }
                                    })
                        .withStoreOption(Option.RETURN_BODY, true)
                        .build();
        
        UpdateValue.Response response = client.execute(uv);
        RiakObject ro = response.getValues().get(0);
        assertNotNull(ro.getVClock());
    }
    
    public static class GenericPojo<T>
    {
        public T value;
        public List<T> list;
        
        @RiakVClock
        public VClock vclock;
    }
    
    public static class Foo
    {
        public String fooValue;
        @RiakVClock
        public VClock vclock;
        
        public Foo() {}
        
        public Foo(String v)
        {
            this.fooValue = v;
        }

        @Override
        public int hashCode()
        {
            int hash = 7;
            hash = 89 * hash + (this.fooValue != null ? this.fooValue.hashCode() : 0);
            return hash;
        }
        
        @Override
        public boolean equals(Object o)
        {
            Foo other = (Foo)o;
            return fooValue.equals(other.fooValue);
        }
        
    }
    
    public static class MyUpdate extends Update<GenericPojo<Integer>>
    {

        @Override
        public GenericPojo<Integer> apply(GenericPojo<Integer> original)
        {
            if (original == null)
            {
                original = new GenericPojo<Integer>();
                original.value = 1;
            }
            else
            {
                original.value++;
            }
            
            return original;
        }
    }
    
    public static class MyResolver implements ConflictResolver<GenericPojo<Integer>>
    {

        @Override
        public GenericPojo<Integer> resolve(List<GenericPojo<Integer>> objectList) throws UnresolvedConflictException
        {
            int total = 0;
            for (GenericPojo<Integer> gpi : objectList)
            {
                total += gpi.value;
            }
            GenericPojo<Integer> newObj = new GenericPojo<Integer>();
            newObj.value = total;            
            return newObj;
        }
        
    }
    
    public static class MyConverter extends Converter<GenericPojo<Integer>>
    {

        public MyConverter(Type t)
        {
            super(t);
        }
        
        @Override
        public GenericPojo<Integer> toDomain(BinaryValue value, String contentType) throws ConversionException
        {
            GenericPojo<Integer> gpi = new GenericPojo<Integer>();
            gpi.value = Integer.valueOf(value.toString());
            return gpi;
        }

        @Override
        public ContentAndType fromDomain(GenericPojo<Integer> domainObject) throws ConversionException
        {
            return new ContentAndType(BinaryValue.create(String.valueOf(domainObject.value)), "text/plain");
        }
        
    }
    
    public class MyFooUpdate extends Update<Foo>
    {

        @Override
        public Foo apply(Foo original)
        {
            if (original == null)
            {
                original = new Foo("Little");
            }
            else
            {
                if (original.fooValue.endsWith("Little"))
                {
                    original.fooValue = original.fooValue + " bunny";
                }
                else if (original.fooValue.endsWith("bunny"))
                {
                    original.fooValue = original.fooValue + " foo foo.";
                }
            }
            return original;
        }
    }
    
    public class MyFooResolver implements ConflictResolver<Foo>
    {

        @Override
        public Foo resolve(List<Foo> objectList) throws UnresolvedConflictException
        {
            if (objectList.isEmpty())
            {
                return null;
            }
            else
            {
                int longest = 0;
                for (int i = 0; i < objectList.size(); i++)
                {
                    if (objectList.get(i).fooValue.length() > longest)
                    {
                        longest = i;
                    }
                }
                return objectList.get(longest);
            }
        }
    }
    
    public static class MyFooConverter extends Converter<Foo>
    {
        public MyFooConverter()
        {
            super(Foo.class);
        }
        @Override
        public Foo toDomain(BinaryValue value, String contentType) throws ConversionException
        {
            return new Foo(value.toString());
        }

        @Override
        public ContentAndType fromDomain(Foo domainObject) throws ConversionException
        {
            return new ContentAndType(BinaryValue.create(domainObject.fooValue), "text/plain");
        }
        
    }
    
}
