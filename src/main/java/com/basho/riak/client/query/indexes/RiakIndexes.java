/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.query.RiakObject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages {@link RiakIndex} objects to be used with a {@link RiakObject}.
 * <p>
 * This container manages and allows for an arbitrary number and types of {@link RiakIndex}s to
 * be used with a {@link RiakObject}. 
 * </p>
 * <h4>Working with RiakIndexes</h4>
 * <p>Data in Riak, including secondary indexes, is stored as raw bytes. The conversion
 * to and from bytes is handled by the concrete {@code RiakIndex} implementations 
 * and all indexes are managed by this container. </p>
 * <p>
 * Each concrete {@code RiakIndex} includes a hybrid builder class named {@code Name}. 
 * The methods of this class take an instance of that builder as an 
 * argument to allow for proper type inference and construction of {@code RiakIndex}
 * objects to expose. 
 * </p>
 * <p>{@code getIndex()} will either return a reference to 
 * the existing {@code RiakIndex} or atomically add and return a new one. The
 * returned reference is of the type provided by the {@code Name} and is the 
 * mutable index; changes are made directly to it.
 * </p>
 * <blockquote><pre>
 * LongIntIndex myIndex = riakIndexes.getIndex(new LongIntIndex.Name("number_on_hand"));
 * myIndex.removeAll();
 * myIndex.add(6L);
 * </pre></blockquote>
 * <p>Calls can be chained, allowing for easy addition or removal of values from 
 * an index.
 * </p>
 * <blockquote><pre>
 * riakIndexes.getIndex(new StringBinIndex.Name("colors")).remove("blue").add("red");
 * </pre></blockquote>
 * @riak.threadsafety This is a thread safe container. 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakObject#getIndexes() 
 * @see RiakObject#setIndexes(com.basho.riak.client.query.indexes.RiakIndexes) 
 */
public class RiakIndexes 
{
    private final ConcurrentHashMap<String, RiakIndex<?>> indexes = 
        new ConcurrentHashMap<String, RiakIndex<? extends Object>>(); 
    
    /**
     * Instantiates a new RiakIndexes object containing no RiakIndex objects
     */
    public RiakIndexes()
    {
        
    }
    
    /**
     * Return the number of indexes present
     * @return the number of indexes
     */
    public int size()
    {
        return indexes.size();
    }
    
    /**
     * Returns whether any {@code RiakIndex} objects are present
     * @return {@code true} if there are indexes, {@code false} otherwise
     */
    public boolean isEmpty()
    {
        return indexes.isEmpty();
    }
    
    /**
     * Returns whether a specific RiakIndex is present
     * @param name the {@link RiakIndex.Name} representing the index to check for
     * @return {@code true} if the index is present, {@code false} otherwise
     */
    public <T extends RiakIndex> boolean hasIndex(RiakIndex.Name<T> name)
    {
        return indexes.containsKey(name.getFullname());
    }
    
    /**
     * Get the named RiakIndex
     * <p>
     * If the named index does not exist it is created and added to the container
     * before being returned. 
     * </p>
     * @param name The {@link RiakIndex.Name} of the index to retrieve
     * @return The requested index typed accordingly.
     */
    public  <V extends RiakIndex, T extends RiakIndex.Name<V>> V getIndex(T name)
    {
        RiakIndex<?> existing = indexes.get(name.getFullname());
        if (existing != null)
        {
            return name.wrap(existing).createIndex();
        }
        else
        {
            V newIndex = name.createIndex();
            existing = indexes.putIfAbsent(newIndex.getFullname(), newIndex);
            if (existing != null)
            {
                return getIndex(name);
            }
            else
            {
                return newIndex;
            }
        }
    }
    
    /**
     * Remove the named RiakIndex 
     * @param name the {@code RiakIndex.Name} representing the index to remove
     * @return the removed {@code RiakIndex} (typed accordingly) if the index was present, 
     *  {@code null} otherwise
     */
    public <V,T extends RiakIndex<V>> T removeIndex(RiakIndex.Name<T> name)
    {
        RiakIndex<?> removed = indexes.remove(name.getFullname());
        if (removed != null)
        {
            T index = name.wrap(removed).createIndex();
            return index;
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Remove all indexes.
     */
    public void removeAllIndexes()
    {
        indexes.clear();
    }
    
}
