/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.api.convert;

import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.StringConverter;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.api.convert.JSONConverter;
import com.basho.riak.client.api.convert.ConversionException;
import com.basho.riak.client.api.convert.PassThroughConverter;
import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.convert.Converter.OrmExtracted;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ConverterFactoryTest
{
    @Before
    public void setup()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        factory.unregisterConverterForClass(Pojo.class);
    }

    @Test
    public void getDefaultConverter()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        Converter<Pojo> converter = factory.getConverter(Pojo.class);

        assertTrue(converter instanceof JSONConverter);

        Pojo pojo = new Pojo();
        pojo.foo = "foo_value";
        pojo.bar = "bar_value";

        OrmExtracted orm = converter.fromDomain(pojo , null, null);
        RiakObject ro = orm.getRiakObject();

        assertNotNull(ro.getValue());

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
        Pojo pojo2 = converter.toDomain(ro, new Location(ns, "key"));

        assertEquals(pojo.foo, pojo2.foo);
        assertEquals(pojo.bar, pojo2.bar);
    }

    @Test
    public void stringConverter()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        Converter<String> converter = factory.getConverter(String.class);

        assertTrue(converter instanceof StringConverter);
    }

    @Test
    public void riakObjectConverter()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        Converter<RiakObject> converter = factory.getConverter(RiakObject.class);

        assertTrue(converter instanceof PassThroughConverter);

        RiakObject o = new RiakObject();

        RiakObject o2 = converter.toDomain(o, null);
        assertEquals(o, o2);

        OrmExtracted orm = converter.fromDomain(o, null, null);
        assertEquals(o, orm.getRiakObject());
    }

    @Test
    public void registerConverterClass()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        factory.registerConverterForClass(Pojo.class, new MyConverter());

        Converter<Pojo> converter = factory.getConverter(Pojo.class);

        assertTrue(converter instanceof MyConverter);
    }

    @Test
    public void registerConverterInstance()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        MyConverter converter = new MyConverter();
        factory.registerConverterForClass(Pojo.class, converter);

        Converter<Pojo> converter2 = factory.getConverter(Pojo.class);
        assertTrue(converter2 instanceof MyConverter);
        assertEquals(converter, converter2);
    }

    public static class MyConverter extends Converter<Pojo>
    {
        public MyConverter()
        {
            super(new TypeReference<Pojo>() {}.getType());
        }

        @Override
        public Pojo toDomain(BinaryValue value, String contentType) throws ConversionException
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public ContentAndType fromDomain(Pojo domainObject) throws ConversionException
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }

    public static class Pojo
    {
        public Pojo() {}

        @RiakBucketName
        String bucketName = "my_bucket";

        @RiakVClock
        VClock vclock;

        @JsonProperty
        String foo;
        @JsonProperty
        String bar;
    }
}
