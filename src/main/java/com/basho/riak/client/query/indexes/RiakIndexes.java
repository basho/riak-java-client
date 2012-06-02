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
    private final ConcurrentMap<IntIndex, Set<Integer>> intIndexes = new ConcurrentHashMap<IntIndex, Set<Integer>>();

    public RiakIndexes(final Map<BinIndex, Set<String>> binIndexes, final Map<IntIndex, Set<Integer>> intIndexes) {
        for (Map.Entry<BinIndex, Set<String>> bi : binIndexes.entrySet()) {
            Set<String> v = bi.getValue();
            if (v != null) {
                this.binIndexes.put(bi.getKey(), new HashSet<String>(bi.getValue()));
            }
        }

        for (Map.Entry<IntIndex, Set<Integer>> ii : intIndexes.entrySet()) {
            Set<Integer> v = ii.getValue();
            if (v != null) {
                this.intIndexes.put(ii.getKey(), new HashSet<Integer>(ii.getValue()));
            }
        }
    }

    /**
     * 
     */
    public RiakIndexes() {}

    /**
     * @return a *copy* of the {@link BinIndex}s
     */
    public Map<BinIndex, Set<String>> getBinIndexes() {
        return new HashMap<BinIndex, Set<String>>(binIndexes);
    }

    /**
     * @return a *copy* of the {@link IntIndex}s
     */
    public Map<IntIndex, Set<Integer>> getIntIndexes() {
        return new HashMap<IntIndex, Set<Integer>>(intIndexes);
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
    public RiakIndexes add(final String index, final String value) {
        final BinIndex key = BinIndex.named(index);
        final String lock = key.getFullname().intern();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lock) {
            Set<String> values = binIndexes.get(key);
            if (values == null) {
                values = new HashSet<String>();
                binIndexes.put(key, values);
            }
            values.add(value);
        }
        return this;
    }

    /**
     * Add a Set of {@link BinIndex} values to the set
     * 
     * @param index
     *            name of the index
     * @param indexValues
     *            the values
     * @return this
     */
    public RiakIndexes addBinSet(final String index, final Set<String> indexValues) {
        if (!indexValues.isEmpty()) {
            final BinIndex key = BinIndex.named(index);
            final String lock = key.getFullname().intern();
            // even though it is a concurrent hashmap, we need
            // fine grained access control for the
            // key's value set
            synchronized (lock) {
                Set<String> values = binIndexes.get(key);
                if (values == null) {
                    // This will create the proper capacity for every use case.
                    values = new HashSet<String>(Math.max((int) (indexValues.size() / .75f) + 1, 16));
                    binIndexes.put(key, values);
                }
                for (String value : indexValues) {
                    values.add(value);
                }
            }
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
    public RiakIndexes add(final String index, final int value) {
        final IntIndex key = IntIndex.named(index);
        final String lock = key.getFullname().intern();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lock) {
            Set<Integer> values = intIndexes.get(key);
            if (values == null) {
                values = new HashSet<Integer>();
                intIndexes.put(key, values);
            }
            values.add(value);
        }
        return this;
    }

    /**
     * Add a Set of {@link IntIndex} values to the set
     * 
     * @param index
     *            name of the index
     * @param indexValues
     *            the values
     * @return this
     */
    public RiakIndexes addIntSet(final String index, final Set<Integer> indexValues) {
        if (!indexValues.isEmpty()) {
            final IntIndex key = IntIndex.named(index);
            final String lock = key.getFullname().intern();
            // even though it is a concurrent hashmap, we need
            // fine grained access control for the
            // key's value set
            synchronized (lock) {
                Set<Integer> values = intIndexes.get(key);
                if (values == null) {
                    // This will create the proper capacity for every use case.
                    values = new HashSet<Integer>(Math.max((int) (indexValues.size() / .75f) + 1, 16));
                    intIndexes.put(key, values);
                }
                for (Integer value : indexValues) {
                    values.add(value);
                }
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
    public Set<Integer> getIntIndex(String name) {
        Set<Integer> values = intIndexes.get(IntIndex.named(name));
        if (values == null) {
            return new HashSet<Integer>();
        }
        return new HashSet<Integer>(values);
    }
}
