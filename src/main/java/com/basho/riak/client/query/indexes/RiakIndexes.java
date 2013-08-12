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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread safe container for {@link RiakIndex} objects.
 * 
 * @riak.threadsafety This is a thread safe container. 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakObject#getIndexes() 
 * @see RiakObject#setIndexes(com.basho.riak.client.query.indexes.RiakIndexes) 
 */
public class RiakIndexes 
{
    private final Map<String, RiakIndex<?>> indexes = 
        new ConcurrentHashMap<String, RiakIndex<? extends Object>>(); 
    
    /**
     * Instantiates a new RiakIndexes object containing no RiakIndex objects
     */
    public RiakIndexes()
    {
        
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
     * Returns whether a specific index is present
     * @param index the {@link RiakIndex} to check for
     * @return {@code true} if the index is present, {@code false} otherwise
     */
    public boolean hasIndex(RiakIndex<?> index)
    {
        return indexes.containsKey(index.getFullname());
    }
    
    /**
     * Get an index 
     * @param index The {@link RiakIndex} to retrieve
     * @return The index, or {@code null} if it is not present
     */
    public <T> RiakIndex<T> getIndex(RiakIndex<T> index)
    {
        RiakIndex<?> existing = indexes.get(index.getFullname());
        if (existing != null)
        {
            return index.wrap(existing);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Remove an index
     * @param index the {@code RiakIndex} to remove
     * @return the removed {@code RiakIndex} if the index was present, 
     *  {@code null} otherwise
     */
    public <T> RiakIndex<T> removeIndex(RiakIndex<T> index)
    {
        RiakIndex<?> removed = indexes.remove(index.getFullname());
        if (removed != null)
        {
            return index.wrap(removed);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Remove all indexes
     */
    public void removeAllIndexes()
    {
        indexes.clear();
    }
    
    /**
     * Add a {@link RiakIndex} 
     *
     * If this index already exists, it is replaced.
     * @param index The {@code RiakIndex} to be added. 
     * @return a reference to this object
     */
    public RiakIndexes addIndex(RiakIndex<?> index)
    {
        RawIndex copy = RawIndex.named(index.getName(), index.getType());
        copy.copyFrom(index);
        indexes.put(copy.getFullname(), copy);
        return this;
    }
    
    /**
     * Add a value to an index
     * <p>
     * If the index does not exist, it is created and added
     * </p>
     * @param index the {@code RiakIndex} to which to add the supplied value.
     * @param value the new index value
     * @return a reference to this object
     */
    public <T> RiakIndexes addToIndex(RiakIndex<T> index, T value)
    {
        RiakIndex<?> existing = indexes.get(index.getFullname());
        if (existing != null)
        {
            index.wrap(existing).add(value);
        }
        else
        {
            index.add(value);
            indexes.put(index.getFullname(), index);
        }
        return this;
    }
    
}
