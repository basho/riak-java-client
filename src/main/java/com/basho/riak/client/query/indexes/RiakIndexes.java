/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.query.indexes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.basho.riak.client.IRiakObject;

/**
 * Container for the set of index/values for a {@link IRiakObject}
 * 
 * @author russell
 * 
 */
public class RiakIndexes {

    private final ConcurrentMap<BinIndex, Set<String>> binIndexes = new ConcurrentHashMap<BinIndex, Set<String>>();
    private final ConcurrentMap<IntIndex, Set<Long>> intIndexes = new ConcurrentHashMap<IntIndex, Set<Long>>();

    public RiakIndexes(final Map<BinIndex, Set<String>> binIndexes, final Map<IntIndex, Set<Long>> intIndexes) {
        for (Map.Entry<BinIndex, Set<String>> bi : binIndexes.entrySet()) {
            Set<String> v = bi.getValue();
            if (v != null) {
                this.binIndexes.put(bi.getKey(), new HashSet<String>(bi.getValue()));
            }
        }

        for (Map.Entry<IntIndex, Set<Long>> ii : intIndexes.entrySet()) {
            Set<Long> v = ii.getValue();
            if (v != null) {
                this.intIndexes.put(ii.getKey(), new HashSet<Long>(ii.getValue()));
            }
        }
    }

    /**
     * 
     */
    public RiakIndexes() {}

    /**
     * @return a *shallow copy* of the {@link BinIndex}s
     */
    public Map<BinIndex, Set<String>> getBinIndexes() {
        return new HashMap<BinIndex, Set<String>>(binIndexes);
    }

    /**
     * @return a *shallow copy* of the {@link IntIndex}s
     */
    public Map<IntIndex, Set<Long>> getIntIndexes() {
        return new HashMap<IntIndex, Set<Long>>(intIndexes);
    }

    /**
     * Add a new {@link BinIndex} value to the set
     * 
     * @param index
     *            the index name
     * @param value
     *            the value
     * @return this
     */
    public RiakIndexes add(String index, String value) {
        final BinIndex key = BinIndex.named(index);
        Set<String> newSet = new HashSet<String>();
        Set<String> prevSet = binIndexes.putIfAbsent(key, newSet);
        Set<String> values = prevSet == null ? newSet : prevSet;
        synchronized (values) { 
            values.add(value);
        }
        return this;
    }

    /**
     * Add a new {@link BinIndex} set of values to the set
     * 
     * @param index
     *            the index name
     * @param values
     *            the set of values
     * @return this
     */    
    public RiakIndexes addBinSet(String index, Set<String> newValues) {
        
        final BinIndex key = BinIndex.named(index);
        Set<String> newSet = new HashSet<String>();
        Set<String> prevSet = binIndexes.putIfAbsent(key, newSet);
        Set<String> values = prevSet == null ? newSet : prevSet;
        synchronized (values) { 
            values.addAll(newValues);
        }
        return this;
    }
    
    
    /**
     * Add a new {@link IntIndex} value to the set
     * 
     * @param name
     *            name of the index
     * @param value
     *            the value
     * @return this
     */
    public RiakIndexes add(String index, long value) {
        final IntIndex key = IntIndex.named(index);
        Set<Long> newSet = new HashSet<Long>();
        Set<Long> prevSet = intIndexes.putIfAbsent(key, newSet);
        Set<Long> values = prevSet == null ? newSet : prevSet;
        synchronized (values) { 
            values.add(value);
        }
        return this;
    }

    /**
     * Add a new set of {@link IntIndex} values to the set
     * 
     * @param name
     *            name of the index
     * @param values
     *            the set of values
     * @return this
     */
    public RiakIndexes addIntSet(String index, Set<? extends Number> newValues) {
        final IntIndex key = IntIndex.named(index);
        Set<Long> newSet = new HashSet<Long>();
        Set<Long> prevSet = intIndexes.putIfAbsent(key, newSet);
        Set<Long> values = prevSet == null ? newSet : prevSet;
        synchronized (values) { 
            // This was done when changing internals from Integer to Long to preserve
            // external usage
            for (Number n : newValues) {
                values.add(n.longValue());
            }
        }
        return this;
    }
    
    /**
     * Remove a {@link BinIndex}
     * 
     * @param index
     *            the {@link BinIndex} to remove
     */
    public RiakIndexes removeAll(BinIndex index) {
        binIndexes.remove(index);
        return this;
    }

    /**
     * Remove the IntIndex
     * 
     * @param index
     *            the {@link IntIndex} to remove
     */
    public RiakIndexes removeAll(IntIndex index) {
        intIndexes.remove(index);
        return this;
    }

    /**
     * Copy a RiakIndexes to a new instance
     * 
     * @param indexes
     * @return a copy of this RiakIndexes
     */
    public static RiakIndexes from(RiakIndexes indexes) {
        return new RiakIndexes(indexes.getBinIndexes(), indexes.getIntIndexes());
    }

    /**
     * Get a copy of the set of values for a given binary index
     * 
     * @param name
     *            the name of the index
     * @return a copy of the values (or the empty set if index is not present)
     */
    public Set<String> getBinIndex(String name) {
        Set<String> values = binIndexes.get(BinIndex.named(name));
        if (values == null) {
            return new HashSet<String>();
        }
        return new HashSet<String>(values);
    }

    /**
     * Get a copy of the set of values for a given int index
     * 
     * @param name
     *            the name of the index
     * @return a copy of the values (or the empty set if index is not present)
     */
    public Set<Long> getIntIndex(String name) {
        Set<Long> values = intIndexes.get(IntIndex.named(name));
        if (values == null) {
            return new HashSet<Long>();
        }
        return new HashSet<Long>(values);
    }
}
