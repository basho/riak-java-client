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

package com.basho.riak.client.cap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public enum ConflictResolverFactory
{
    INSTANCE;
    
    private final Map<Class<?>, ConflictResolver<?>> resolverInstances =
        new ConcurrentHashMap<Class<?>, ConflictResolver<?>>();
    
    private final Class<? extends ConflictResolver> defaultResolver = DefaultResolver.class;
    
    /**
     * Returns the instance of the ConflictResolverFactory.
     * @return The ConflictResolverFactory
     */
    public static ConflictResolverFactory getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Return the ConflictResolver for the given class.
     * <p>
     * If no ConflictResolver is registered for the provided class, an instance of the 
     * {@link com.basho.riak.client.cap.DefaultResolver} is returned. 
     * </p>
     * @param <T> The type being resolved
     * @param clazz the class of the type being resolved
     * @return The conflict resolver for the type.
     * @throws UnresolvedConflictException 
     */
    @SuppressWarnings("unchecked")
    public <T> ConflictResolver<T> getConflictResolverForClass(Class<T> clazz) throws UnresolvedConflictException
    {
        if (clazz == null)
        {
            throw new IllegalArgumentException("clazz cannot be null");
        }
        else
        {
            ConflictResolver<T> resolver = (ConflictResolver<T>) resolverInstances.get(clazz);
            if (resolver == null)
            {
                try
                {
                    resolver = (ConflictResolver<T>) defaultResolver.newInstance();
                }
                catch(Exception ex)
                {
                    throw new UnresolvedConflictException(ex, "Could not instantiate resolver", null);
                }
            }
            
            return resolver;
            
        }
    }
    
    /**
     * Register a ConflictResolver.
     * <p>
     * The instance provided will be used to resolve siblings for the given type.
     * </p>
     * 
     * @param <T> The type being resolved
     * @param clazz the class of the type being resolved
     * @param resolver an instance of a class implementing ConflictResolver.
     */
    public <T> void registerConflictResolverForClass(Class<T> clazz, ConflictResolver<T> resolver)
    {
        resolverInstances.put(clazz, resolver);
    }
    
    /**
     * Unregister a ConflictResolver.
     * @param <T> The type being Resolved
     * @param clazz the class of the type being resolved.
     */
    public <T> void unregisterConflictResolverForClass(Class<T> clazz)
    {
        resolverInstances.remove(clazz);
    }
    
}
