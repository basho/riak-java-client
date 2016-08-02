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

package com.basho.riak.client.api.convert;

import com.basho.riak.client.core.query.RiakObject;
import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds instances of converters to be used for serialization / deserialization 
 * of objects stored and fetched from Riak.
 * <p>
 * When storing and retrieving your own domain objects to/from Riak, they
 * need to be serialized / deserialized. By default the {@link JSONConverter}
 * is provided. This uses the Jackson JSON library to translate your object to/from
 * JSON. In many cases you will never need to create or register your own
 * converter with the ConverterFactory.
 * </p>
 * <p>
 * In the case you do need custom conversion, you would extend {@link Converter}
 * and then register it with the ConverterFactory for your classes.
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since  2.0
 */
public enum ConverterFactory 
{
    INSTANCE;
    
    private final Map<Type, Converter<?>> converterInstances =
        new ConcurrentHashMap<Type, Converter<?>>()
        {{
            put(new TypeReference<RiakObject>(){}.getType(), new PassThroughConverter());
            put(RiakObject.class, new PassThroughConverter());
            put (new TypeReference<String>(){}.getType(), new StringConverter());
            put(String.class, new StringConverter());
        }};
    
    
    
    /**
     * Get the instance of the ConverterFactory. 
     * @return the ConverterFactory
     */
    public static ConverterFactory getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Returns a Converter<T> instance for the supplied class.
     * <p>
     * If no converter is registered, the default {@link JSONConverter} is returned.
     * </p>
     * @param <T> The type for the converter
     * @param type The type used to look up the converter
     * @return an instance of the converter for this class
     */
    public <T> Converter<T> getConverter(Type type)
    {
        return getConverter(type, null);
    }
    
    /**
     * Returns a Converter instance for the supplied class.
     * <p>
     * If no converter is registered, the default {@link JSONConverter} is returned.
     * </p>
     * @param <T> The type for the converter
     * @param typeReference the TypeReference for the class being converted.
     * @return an instance of the converter for this class
     */
    public <T> Converter<T> getConverter(TypeReference<T> typeReference)
    {
        return getConverter(null, typeReference);
    }
    
    @SuppressWarnings("unchecked")
    private <T> Converter<T> getConverter(Type type, TypeReference<T> typeReference) 
    {
        type = type != null ? type : typeReference.getType();
        
        Converter<T> converter;
        
        converter = (Converter<T>) converterInstances.get(type);
        if (converter == null)
        {
            if (typeReference != null)
            {
                // Should we cache this?
                converter = new JSONConverter<T>(typeReference);
            }
            else
            {
                converter = new JSONConverter<T>(type);
            }
        }

        return converter;
        
    }
    
    
    /**
     * Register a converter for the supplied class.
     * <p>
     * This instance be re-used for every conversion.
     * </p>
     * @param <T> The type being converted
     * @param clazz the class for this converter.
     * @param converter an instance of Converter
     */
    public <T> void registerConverterForClass(Class<T> clazz, Converter<T> converter)
    {
        converterInstances.put((Type)clazz, converter);
    }
    
    /**
     * Register a converter for the supplied class.
     * <p>
     * This instance be re-used for every conversion.
     * </p>
     * @param <T> The type being converted
     * @param typeReference the TypeReference for the class being converted.
     * @param converter an instance of Converter
     */
    public <T> void registerConverterForClass(TypeReference<T> typeReference, Converter<T> converter)
    {
        Type t = typeReference.getType();
        converterInstances.put(t, converter);
    }
    
    /**
     * Unregister a converter.
     * @param <T> The type
     * @param clazz the class being converted
     */
    public <T> void unregisterConverterForClass(Class<T> clazz)
    {
        converterInstances.remove((Type)clazz);
    }
    
    /**
     * Unregister a converter.
     * @param <T> The type
     * @param typeReference the TypeReference for the class being converted.
     */
    public <T> void unregisterConverterForClass(TypeReference<T> typeReference)
    {
        Type t = typeReference.getType();
        converterInstances.remove(t);
    }
}
