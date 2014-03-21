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

package com.basho.riak.client.convert;

import com.basho.riak.client.convert.Converter.OrmExtracted;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
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
        //factory.setDefaultConverter(JSONConverter.class);
    }
    
    @Test
    public void getDefaultConverter()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        //TypeReference tr = new TypeReference<Pojo>(){};
        Converter<Pojo> converter = factory.getConverter(Pojo.class);
        
        assertTrue(converter instanceof JSONConverter);
    
        Pojo pojo = new Pojo();
        pojo.foo = "foo_value";
        pojo.bar = "bar_value";
        
        
        OrmExtracted orm = converter.fromDomain(pojo , null, null);
        RiakObject ro = orm.getRiakObject();
        
        assertNotNull(ro.getValue());
        
        Pojo pojo2 = converter.toDomain(ro, new Location((String)null), null);
        
        assertEquals(pojo.foo, pojo2.foo);
        assertEquals(pojo.bar, pojo2.bar);
        
    }
    
    @Test
    public void riakObjectConverter()
    {
        ConverterFactory factory = ConverterFactory.getInstance();
        Converter<RiakObject> converter = factory.getConverter(RiakObject.class);
        
        assertTrue(converter instanceof PassThroughConverter);
        
        RiakObject o = new RiakObject();
        
        RiakObject o2 = converter.toDomain(o, null, null);
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
            super(new TypeReference<Pojo>(){}.getType());
        }
        
        
        
        @Override
        public Pojo toDomain(BinaryValue value, String contentType) throws ConversionException
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public BinaryValue fromDomain(Pojo domainObject, String contentType) throws ConversionException
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
    
    public static class Pojo
    {
        public Pojo(){}
        
        @JsonProperty
        String foo;
        @JsonProperty
        String bar;
    }
    
}
