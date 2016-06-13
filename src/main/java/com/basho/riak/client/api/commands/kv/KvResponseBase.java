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

package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.api.convert.reflection.AnnotationUtil;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Base abstract class that KV responses extend.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
abstract class KvResponseBase
{
    private final Location location;
    private final List<RiakObject> values;
    
        
    protected KvResponseBase(Init<?> builder)
    {
        this.location = builder.location;
        this.values = builder.values;
    }

    /**
     * Determine if this response contains any returned values.
     * @return true if values are present, false otherwise.
     */
    public boolean hasValues()
    {
        return !values.isEmpty();
    }
    
    /**
     * Return the number of values contained in this response.
     * <p>
     * If siblings are present at the {@code Location}, all values
     * are returned.
     * <p>
     * @return the number of values in this response.
     */
    public int getNumberOfValues()
    {
        return values.size();
    }
    
    /**
     * Get all the objects returned in this response.
     * <p>
     * If siblings were present in Riak for the object you were fetching, 
     * this method will return all of them to you.
     * </p>
     * @return a list of values as RiakObjects
     */
    public List<RiakObject> getValues()
    {
        return values;
    }
    
    /**
     * Get the vector clock returned with this response.
     * <p>
     * When storing/retrieving core Java types ({@code HashMap},
     * {@code ArrayList},{@code String}, etc) or non-annotated POJOs 
     * this method allows you to retrieve the vector clock.
     * </p>
     * @return The vector clock or null if one is not present.
     */
    public VClock getVectorClock()
    {
        if (hasValues())
        {
            return values.get(0).getVClock();
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Get all the objects returned in this response.
     * <p>
     * If siblings were present in Riak for the object you were fetching, 
     * this method will return all of them to you.
     * </p>
     * <p>
     * The values will be converted to the supplied class using the 
     * {@link com.basho.riak.client.api.convert.Converter} returned from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
     * By default this will be the {@link com.basho.riak.client.api.convert.JSONConverter},
     * or no conversion at all if you pass in {@code RiakObject.class}. 
     * </p>
     * @param clazz the class to be converted to
     * @return a list of values, converted to the supplied class.
     * @see ConverterFactory
     * @see Converter
     */
    public <T> List<T> getValues(Class<T> clazz)
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(clazz);
        return convertValues(converter);
    }

    /**
     * Get all the objects returned in this response.
     * <p>
     * If siblings were present in Riak for the object you were fetching, 
     * this method will return all of them to you.
     * </p>
     * <p>
     * The values will be converted to an object using the supplied
     * {@link com.basho.riak.client.api.convert.Converter} rather than one 
     * registered with the {@link com.basho.riak.client.api.convert.ConverterFactory}.
     * </p>
     * @param converter The converter to use.
     * @return a list of values, converted to the supplied class.
     * @see Converter
     */ 
    public <T> List<T> getValues(Converter<T> converter)
    {
        return convertValues(converter);
    }
    
    /**
     * Get a single, resolved object from this response.
     * <p>
     * The values will be converted to objects using the supplied
     * {@link com.basho.riak.client.api.convert.Converter} rather than one registered 
     * with the {@link com.basho.riak.client.api.convert.ConverterFactory}. 
     * </p>
     * <p>If there are multiple 
     * values present (siblings), they will then be resolved using the supplied
     * {@link com.basho.riak.client.api.cap.ConflictResolver} rather than one 
     * registered with the {@link com.basho.riak.client.api.cap.ConflictResolverFactory}.  
     * </p>
     * @param converter The converter to use.
     * @param resolver The conflict resolver to use. 
     * @return the single, resolved value.
     * @throws UnresolvedConflictException if the resolver fails to resolve siblings.
     * @see Converter
     * @see ConflictResolver
     */ 
    public <T> T getValue(Converter<T> converter, ConflictResolver<T> resolver) throws UnresolvedConflictException
    {
        List<T> convertedValues = convertValues(converter);
        T resolved = resolver.resolve(convertedValues);
        
        if (hasValues() && resolved != null)
        {
            VClock vclock = values.get(0).getVClock();
            AnnotationUtil.setVClock(resolved, vclock);
        }
        
        return resolved;
    }
    
    /**
     * Get a single, resolved object from this response.
     * <p>
     * The values will be converted to the supplied class using the 
     * {@link com.basho.riak.client.api.convert.Converter} returned from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
     * By default this will be the {@link com.basho.riak.client.api.convert.JSONConverter},
     * or no conversion at all if you pass in {@code RiakObject.class}. If there are multiple 
     * values present (siblings), they will then be resolved using the 
     * {@link com.basho.riak.client.api.cap.ConflictResolver} returned by the {@link com.basho.riak.client.api.cap.ConflictResolverFactory}.
     * </p>
     * @param clazz the class to be converted to.
     * @return the single, resolved value converted to the supplied class.
     * @throws UnresolvedConflictException 
     * @see ConverterFactory
     * @see Converter
     * @see ConflictResolverFactory
     * @see ConflictResolver
     */
    public <T> T getValue(Class<T> clazz) throws UnresolvedConflictException
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(clazz);
        List<T> convertedValues = convertValues(converter);

        ConflictResolver<T> resolver = 
            ConflictResolverFactory.getInstance().getConflictResolver(clazz);

        T resolved = resolver.resolve(convertedValues);
        
        if (hasValues() && resolved != null)
        {
            VClock vclock = values.get(0).getVClock();
            AnnotationUtil.setVClock(resolved, vclock);
        }
        
        return resolved;
    }

    /**
     * Get a single, resolved object from this response.
     * <p>
     * The values will be converted to the supplied class using the 
     * {@link com.basho.riak.client.api.convert.Converter} returned from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
     * By default this will be the {@link com.basho.riak.client.api.convert.JSONConverter},
     * or no conversion at all if you pass in {@code RiakObject.class}. If there are multiple 
     * values present (siblings), they will then be resolved using the 
     * {@link com.basho.riak.client.api.cap.ConflictResolver} returned by the {@link com.basho.riak.client.api.cap.ConflictResolverFactory}.
     * </p>
     * <p>
     * This version should only be used if you're converting to a parameterized 
     * generic domain object. For example:
     * <pre>
     * {@literal TypeReference<MyPojo<String>>} tr = new {@literal TypeReference<MyPojo<String>>}(){};
     * {@literal MyPojo<String>} myPojo = response.getValue(tr);
     * </pre>
     * </p>
     * @param typeReference The TypeReference of the class to be converted to.
     * @return the single, resolved value converted to the supplied class.
     * @throws UnresolvedConflictException 
     * @see ConverterFactory
     * @see Converter
     * @see ConflictResolverFactory
     * @see ConflictResolver
     */
    public <T> T getValue(TypeReference<T> typeReference) throws UnresolvedConflictException
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
        List<T> convertedValues = convertValues(converter);

        ConflictResolver<T> resolver = 
            ConflictResolverFactory.getInstance().getConflictResolver(typeReference);

        T resolved = resolver.resolve(convertedValues);
        if (hasValues() && resolved != null)
        {
            VClock vclock = values.get(0).getVClock();
            AnnotationUtil.setVClock(resolved, vclock);
        }
        
        return resolved;
    }

    /** 
     * Get the objects returned in this response.
     * <p>
     * If siblings were present in Riak for the object you were fetching, 
     * this method will return all of them to you.
     * </p>
     * <p>
     * The values will be converted to the supplied class using the 
     * {@link com.basho.riak.client.api.convert.Converter} returned from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
     * By default this will be the {@link com.basho.riak.client.api.convert.JSONConverter},
     * or no conversion at all if you pass in a TypeReference for {@code RiakObject.class}. 
     * </p>
     * <p>
     * This version should only be used if you're converting to a parameterized 
     * generic domain object. For example:
     * <pre>
     * {@literal TypeReference<MyPojo<String>>} tr = new {@literal TypeReference<MyPojo<String>>}(){};
     * {@literal List<MyPojo<String>>} list = response.getValues(tr);
     * </pre>
     * </p>
     * @param typeReference the TypeReference for the class to be converted to
     * @return a list of values, converted to the supplied class.
     * @see ConverterFactory
     * @see Converter
     */
    public <T> List<T> getValues(TypeReference<T> typeReference)
    {
        Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
        return convertValues(converter);
    }

    private <T> List<T> convertValues(Converter<T> converter)
    {
        List<T> convertedValues = new ArrayList<T>(values.size());
        for (RiakObject ro : values)
        {
            convertedValues.add(converter.toDomain(ro, location));
        }

        return convertedValues;
    }
    

    protected static abstract class Init<T extends Init<T>>
    {
        private Location location;
        private List<RiakObject> values = new ArrayList<RiakObject>();

        protected abstract T self();
        abstract KvResponseBase build();

        T withLocation(Location location)
        {
            this.location = location;
            return self();
        }
        
        T withValues(List<RiakObject> values)
        {
            this.values.addAll(values);
            return self();
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (location != null ? location.hashCode() : 0);
        result = prime * result + (values != null ? values.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof KvResponseBase))
        {
            return false;
        }

        final KvResponseBase other = (KvResponseBase) obj;
        if (this.location != other.location && (this.location == null || !this.location.equals(other.location)))
        {
            return false;
        }
        if (this.values != other.values && (this.values == null || !this.values.equals(other.values)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("{location: %s, values: %s}", location, values);
    }
}
