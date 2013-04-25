/*
 * Copyright 2013 Basho Technologies Inc
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Container for the set of index/values for a {@link com.basho.riak.client.RiakObject}
 * 
 * @author Russel Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
 */
public class RiakIndexes
{
    private final Map<BinIndex, Set<String>> binIndexes = new HashMap<BinIndex, Set<String>>();
    private final Map<IntIndex, Set<Long>> intIndexes = new HashMap<IntIndex, Set<Long>>();
    
    // This class is meant to be threadsafe, but the old implementation 
    // was doing copies via constructors with ConcurrentHashMap which if done concurrently with a 
    // mutation could produce inconsistent results as it has a weakly consistent iterator.
    // Using a real lock solves this issue. 
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    public RiakIndexes(final Map<BinIndex, Set<String>> binIndexes, final Map<IntIndex, Set<Long>> intIndexes)
    {
        for (Map.Entry<BinIndex, Set<String>> bi : binIndexes.entrySet())
        {
            Set<String> v = bi.getValue();
            if (v != null)
            {
                this.binIndexes.put(bi.getKey(), new HashSet<String>(v));
            }
        }

        for (Map.Entry<IntIndex, Set<Long>> ii : intIndexes.entrySet())
        {
            Set<Long> v = ii.getValue();
            if (v != null)
            {
                this.intIndexes.put(ii.getKey(), new HashSet<Long>(v));
            }
        }
    }

    public RiakIndexes()
    {
    }

    /**
     * @return a copy of the {@link BinIndex}s
     */
    public Map<BinIndex, Set<String>> getBinIndexes()
    {
        try
        {
            lock.readLock().lock();
            HashMap<BinIndex, Set<String>> copy = new HashMap<BinIndex, Set<String>>();
            for (Map.Entry<BinIndex, Set<String>> bi : binIndexes.entrySet())
            {
                Set<String> v = bi.getValue();
                copy.put(bi.getKey(), new HashSet<String>(v));
            }
            return copy;
        }
        finally
        {
            lock.readLock().unlock();
        }
        
    }

    /**
     * @return a copy of the {@link IntIndex}s
     */
    public Map<IntIndex, Set<Long>> getIntIndexes()
    {
        try
        {
            lock.readLock().lock();
            HashMap<IntIndex, Set<Long>> copy = new HashMap<IntIndex, Set<Long>>();
            for (Map.Entry<IntIndex, Set<Long>> ii : intIndexes.entrySet())
            {
                Set<Long> v = ii.getValue();
                copy.put(ii.getKey(), new HashSet<Long>(v));
            }
            return copy;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Add a new {@link BinIndex} value to the set
     *
     * @param index the index name
     * @param value the value
     * @return this
     */
    public RiakIndexes add(String index, String value)
    {
        try
        {
            lock.writeLock().lock();
            final BinIndex key = BinIndex.named(index);
            Set<String> values = binIndexes.get(key);
            if (null == values)
            {
                values = new HashSet<String>();
                binIndexes.put(key, values);
            }
            values.add(value);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new {@link BinIndex} set of values to the set
     *
     * @param index the index name
     * @param newValues the set of values
     * @return this
     */
    public RiakIndexes addBinSet(String index, Set<String> newValues)
    {

        try
        {
            lock.writeLock().lock();
            final BinIndex key = BinIndex.named(index);
            Set<String> values = binIndexes.get(key);
            if (null == values)
            {
                values = new HashSet<String>();
                binIndexes.put(key, values);
            }
            values.addAll(newValues);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new {@link IntIndex} value to the set
     *
     * @param name name of the index
     * @param value the value
     * @return this
     */
    public RiakIndexes add(String name, long value)
    {
        try
        {
            lock.writeLock().unlock();
            final IntIndex key = IntIndex.named(name);
            Set<Long> values = intIndexes.get(key);
            if (null == values)
            {
                values = new HashSet<Long>();
                intIndexes.put(key, values);
            }
            values.add(value);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new set of {@link IntIndex} values to the set
     *
     * @param name name of the index
     * @param newValues the set of values
     * @return this
     */
    public RiakIndexes addIntSet(String name, Set<Long> newValues)
    {
        try
        {
            lock.writeLock().lock();
            final IntIndex key = IntIndex.named(name);
            Set<Long> values = intIndexes.get(key);
            if (null == values)
            {
                values = new HashSet<Long>();
                intIndexes.put(key, values);
            }
            values.addAll(newValues);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a {@link BinIndex}
     *
     * @param index the {@link BinIndex} to remove
     */
    public RiakIndexes removeAll(BinIndex index)
    {
        try
        {
            lock.writeLock().lock();
            binIndexes.remove(index);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a value from a {@link BinIndex}
     * 
     * @param indexName the index name
     * @param value the index value to remove
     */
    public RiakIndexes remove(String indexName, String value)
    {
        try
        {
            lock.writeLock().lock();
            Set<String> values = binIndexes.get(BinIndex.named(indexName));
            if (values != null)
            {
                values.remove(value);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return this;
    }
    
    /**
     * Remove the IntIndex
     *
     * @param index the {@link IntIndex} to remove
     */
    public RiakIndexes removeAll(IntIndex index)
    {
        try
        {
            lock.writeLock().lock();
            intIndexes.remove(index);
            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a value from a {@link IntIndex}
     * 
     * @param indexName the index name
     * @param value the index value to remove
     */
    public RiakIndexes remove(String indexName, Long value)
    {
        try
        {
            lock.writeLock().lock();
            Set<Long> values = intIndexes.get(IntIndex.named(indexName));
            if (values != null)
            {
                values.remove(value);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return this;
    }
    
    /**
     * Copy a RiakIndexes to a new instance
     *
     * @param indexes
     * @return a copy of this RiakIndexes
     */
    public static RiakIndexes from(RiakIndexes indexes)
    {
        return new RiakIndexes(indexes.getBinIndexes(), indexes.getIntIndexes());
    }

    /**
     * Get a copy of the set of values for a given binary index
     *
     * @param name the name of the index
     * @return a copy of the values (or the empty set if index is not present)
     */
    public Set<String> getBinIndex(String name)
    {
        try
        {
            lock.readLock().lock();
            Set<String> values = binIndexes.get(BinIndex.named(name));
            if (values == null)
            {
                return new HashSet<String>();
            }
            return new HashSet<String>(values);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Get a copy of the set of values for a given int index
     *
     * @param name the name of the index
     * @return a copy of the values (or the empty set if index is not present)
     */
    public Set<Long> getIntIndex(String name)
    {
        try
        {
            lock.readLock().lock();
            Set<Long> values = intIndexes.get(IntIndex.named(name));
            if (values == null)
            {
                return new HashSet<Long>();
            }
            return new HashSet<Long>(values);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}
