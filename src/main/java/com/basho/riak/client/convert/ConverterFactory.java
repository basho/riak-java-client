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

import com.basho.riak.client.query.RiakObject;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since  2.0
 */
public enum ConverterFactory 
{
    INSTANCE;
    
    private final Map<Class<?>, Class<? extends Converter>> converterClasses =
        new ConcurrentHashMap<Class<?>, Class<? extends Converter>>();
    
    private final Map<Class<?>, Converter<?>> converterInstances =
        new ConcurrentHashMap<Class<?>, Converter<?>>();
    
    private final PassThroughConverter passThroughConverter = new PassThroughConverter();
    private volatile Class<? extends Converter> defaultConverter = JSONConverter.class;
    
    /**
     * Get the instance of the ConverterFactory;
     * @return
     */
    public static ConverterFactory getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Returns a Converter<T> instance for the supplied class.
     * <p>
     * If no converter is registered, the default converter is returned.
     * </p>
     * @param <T> The type for the converter
     * @param clazz the class registered with the factory
     * @return an instance of the converter for this class
     * @see #setDefaultConverter(java.lang.Class) 
     */
    @SuppressWarnings("unchecked")
    public <T> Converter<T> getConverterForClass(Class<T> clazz) 
    {
        if (clazz == null)
        {
            throw new IllegalArgumentException("clazz can not be null");
        }
        else if (clazz.equals(RiakObject.class))
        {
            Converter<T> converter = (Converter<T>) passThroughConverter;
            return converter;
        }
        else
        {
            Converter<T> converter = (Converter<T>) converterInstances.get(clazz);
            if (converter == null)
            {
                try
                {
                    Class<? extends Converter> converterClass =
                        converterClasses.get(clazz);
                    
                    if (converterClass == null)
                    {
                        converterClass = defaultConverter;
                    }
                                        
                    Constructor<?> cons = converterClass.getConstructor(Class.class);
                    converter = (Converter<T>) cons.newInstance(clazz);
                }
                catch (Exception ex)
                {
                    throw new ConversionException("Can not instantiate converter", ex);
                }
            }
            
            return converter;
        }
    }
    
    /**
     * Register a converter for the supplied class.
     * <p>
     * This instance be re-used for every conversion.
     * </p>
     * @param <T> The type being converted
     * @param clazz the class being converted
     * @param converter an instance of Converter<T>
     */
    public <T> void registerConverterForClass(Class<T> clazz, Converter<T> converter)
    {
        converterInstances.put(clazz, converter);
    }
    
    /**
     * Unregister a converter.
     * @param <T> The type
     * @param clazz the class being converted
     */
    public <T> void unregisterConverterForClass(Class<T> clazz)
    {
        converterInstances.remove(clazz);
        converterClasses.remove(clazz);
    }
    
    /**
     * Register a Converter class for the supplied class.
     * <p>
     * A new instance of the supplied Converter<T> class will be created for every
     * conversion.
     * </p>
     * @param clazz the class being converted
     * @param converter the class for the converter
     */
    public void registerConverterForClass(Class<?> clazz, Class<? extends Converter> converter)
    {
        validateConverterClass(converter);
        converterClasses.put(clazz, converter);
    }
    
    /**
     * Set the default converter.
     * <p>
     * If a converter hasn't been registered for a class, the converter provided 
     * here will be used. 
     * </p>
     * <p>
     * By default, this is the {@link com.basho.riak.client.convert.JSONConverter}
     * </p>
     * @param converter the default converter.
     */
    public void setDefaultConverter(Class<? extends Converter> converter)
    {
        validateConverterClass(converter);
        defaultConverter = converter;
    }
    
    private void validateConverterClass(Class<? extends Converter> converterClass) 
    {
        try
        {
            Constructor<?> cons = converterClass.getConstructor(Class.class);
        }
        catch (Exception ex)
        {
            throw new ConversionException("Converter is invalid; no constructor that takes a Class", ex);
        }
    }
    
}
