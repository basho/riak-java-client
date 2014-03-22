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

package com.basho.riak.client.operations.kv;

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.ConflictResolverFactory;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
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
    private final VClock vclock;
    private final List<RiakObject> values;
    
        
    protected KvResponseBase(Init<?> builder)
    {
        this.location = builder.location;
        this.vclock = builder.vclock;
        this.values = builder.values;
    }

    /**
     * Get the location in Riak this response corresponds to.
     * @return the location.
     */
    public Location getLocation()
    {
        return location;
    }
    
    /**
     * Determine if a vclock was returned.
     * @return true if a vclock is present, false otherwise
     */
    public boolean hasVClock()
    {
        return vclock != null;
    }
    
    /**
     * Get the returned Vector Clock.
     * @return the vclock, if present. null otherwise.
     */
    public VClock getVClock()
    {
        return vclock;
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
     * Get the values returned in this response.
     * <p>
     * The values will be converted to the supplied class using the 
     * {@code Converter} returned from the {@code ConverterFactory}.
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
     * Get a single, resolved value from this response.
     * <p>
     * All values will be converted to the supplied class using the 
     * {@coce Converter} returned from the {@code ConverterFactory}. If there are multiple 
     * values present (siblings), they will then be resolved using the 
     * {@code ConflictResolver} returned by the {@code ConflictResolverFactory}.
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

        return resolver.resolve(convertedValues);
    }

    /**
     * Get a single, resolved value from this response.
     * <p>
     * All values will be converted to the supplied class using the 
     * {@coce Converter} returned from the {@code ConverterFactory}. If there are multiple 
     * values present (siblings), they will then be resolved using the 
     * {@code ConflictResolver} returned by the {@code ConflictResolverFactory}.
     * </p>
     * @param typeReference The type to be converted to.
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

        return resolver.resolve(convertedValues);
    }

    /**
     * Get the values returned in this response.
     * <p>
     * The values will be converted to the supplied class using the 
     * {@code Converter} returned from the {@code ConverterFactory}.
     * </p>
     * @param clazz the class to be converted to
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
            convertedValues.add(converter.toDomain(ro, location, vclock));
        }

        return convertedValues;
    }
    

    protected static abstract class Init<T extends Init<T>>
    {
        private Location location;
        private VClock vclock;
        private List<RiakObject> values = new ArrayList<RiakObject>();

        protected abstract T self();
        abstract KvResponseBase build();

        T withLocation(Location location)
        {
            this.location = location;
            return self();
        }
        
        T withVClock(VClock vclock)
        {
            this.vclock = vclock;
            return self();
        }
        
        T withValues(List<RiakObject> values)
        {
            this.values.addAll(values);
            return self();
        }
    }
}
