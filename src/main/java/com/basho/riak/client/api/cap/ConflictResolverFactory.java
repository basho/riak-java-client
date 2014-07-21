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

package com.basho.riak.client.api.cap;

import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public enum ConflictResolverFactory
{
    INSTANCE;
    
    private final Map<Type, ConflictResolver<?>> resolverInstances =
        new ConcurrentHashMap<Type, ConflictResolver<?>>();

    /**
     * Returns the instance of the ConflictResolverFactory.
     * @return The ConflictResolverFactory
     */
    public static ConflictResolverFactory getInstance()
    {
        return INSTANCE;
    }
    
    public <T> ConflictResolver<T> getConflictResolver(Class<T> clazz)
    {
        if (clazz == null)
        {
            throw new IllegalArgumentException("clazz cannot be null");
        }
        return getConflictResolver(clazz, null);
    }
    
    public <T> ConflictResolver<T> getConflictResolver(TypeReference<T> typeReference)
    {
        if (typeReference == null)
        {
            throw new IllegalArgumentException("typeReference cannot be null");
        }
        return getConflictResolver(null, typeReference);
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
    private <T> ConflictResolver<T> getConflictResolver(Type type, TypeReference<T> typeReference) 
    {
        
        type = type != null ? type : typeReference.getType();
        
        ConflictResolver<T> resolver = (ConflictResolver<T>) resolverInstances.get(type);
        if (resolver == null)
        {
            // Cache this?
            resolver = (ConflictResolver<T>) new DefaultResolver<T>();
        }

        return resolver;
            

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
    public <T> void registerConflictResolver(Class<T> clazz, ConflictResolver<T> resolver)
    {
        resolverInstances.put(clazz, resolver);
    }
    
    public <T> void registerConflictResolver(TypeReference<T> typeReference, ConflictResolver<T> resolver)
    {
        resolverInstances.put(typeReference.getType(), resolver);
    }
    
    
    /**
     * Unregister a ConflictResolver.
     * @param <T> The type being Resolved
     * @param clazz the class of the type being resolved.
     */
    public <T> void unregisterConflictResolver(Class<T> clazz)
    {
        resolverInstances.remove(clazz);
    }
 
    public <T> void unregisterConflictResolver(TypeReference<T> typeReference)
    {
        resolverInstances.remove(typeReference.getType());
    }
}
