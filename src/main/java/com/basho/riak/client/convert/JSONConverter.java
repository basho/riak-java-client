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
package com.basho.riak.client.convert;


import java.io.IOException;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import com.basho.riak.client.util.BinaryValue;

/**
 * Converts a RiakObject's value to an instance of T. T must have a field
 * annotated with {@link RiakKey} or you must construct the converter with a key to use. RiakObject's value *must* be a JSON string.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @param <T> type to convert to/from
 * 
 */
public class JSONConverter<T> extends Converter<T> {

    // Object mapper per domain class is expensive, a singleton (and ThreadSafe) will do.
    private static final ObjectMapper OBJECT_MAPPER= new ObjectMapper();
    static {
        OBJECT_MAPPER.registerModule(new RiakJacksonModule());
        OBJECT_MAPPER.registerModule(new JodaModule());
    }
    
    /**
     * Create a JSONConverter for creating instances of <code>clazz</code> from
     * JSON and instances of {@link IRiakObject} with a JSON payload from
     * instances of <code>clazz</code>
     * 
     * @param clazz the type to convert to/from
     */
    public JSONConverter(Class<T> clazz) {
        super(clazz);
    }

    /**
     * Returns the {@link ObjectMapper} being used.
     * This is a convenience method to allow changing its behavior.
     * @return The Jackson ObjectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Convenient method to register a Jackson module into the singleton Object mapper used by domain objects.
     * @param jacksonModule Module to register.
     */
    public static void registerJacksonModule(final Module jacksonModule) {
        OBJECT_MAPPER.registerModule(jacksonModule);
    }

    @Override
    public T toDomain(BinaryValue value, String contentType)
    {
        try
        {
            return OBJECT_MAPPER.readValue(value.unsafeGetValue(), clazz);
        }
        catch (IOException ex)
        {
            throw new ConversionException(ex);
        }
    }

    @Override
    public BinaryValue fromDomain(T domainObject)
    {
        try    
        {
            return BinaryValue.unsafeCreate(OBJECT_MAPPER.writeValueAsBytes(domainObject));
        }
        catch (JsonProcessingException ex)
        {
            throw new ConversionException(ex);
        }
    }

}
