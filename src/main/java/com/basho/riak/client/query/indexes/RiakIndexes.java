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
import java.util.concurrent.ConcurrentMap;

import com.basho.riak.client.IRiakObject;
import com.google.common.collect.Maps;

/**
 * Container for the set of index/values for a {@link IRiakObject}
 * 
 * @author russell
 * 
 */
public class RiakIndexes {

    // Guava concurrent maps have a better performance than JDK built-in ones.
    private final ConcurrentMap<BinIndex, Set<String>> binIndexes = Maps.newConcurrentMap();
    private final ConcurrentMap<IntIndex, Set<Integer>> intIndexes = Maps.newConcurrentMap();
    // A lock map with an initial concurrency level of 16 and capacity of 64.
    // You can tune it up depending on your needs.
    private final LockMap<String> lockMap = new LockMap<String>(16, 64);

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
    public RiakIndexes add(String index, String value) {
        final BinIndex key = BinIndex.named(index);
        final String lockName = key.getFullname();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lockMap.getLock(lockName)) {
            Set<String> values = binIndexes.get(key);
            if (values == null) {
                values = new HashSet<String>();
            }
            values.add(value);
            binIndexes.put(key, values);
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
        final String lockName = key.getFullname();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lockMap.getLock(lockName)) {

            Set<String> values = binIndexes.get(key);

            if (values == null) {
                values = new HashSet<String>();
            }

            values.addAll(newValues);
            binIndexes.put(key, values);
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
    public RiakIndexes add(String index, int value) {
        final IntIndex key = IntIndex.named(index);
        final String lockName = key.getFullname();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lockMap.getLock(lockName)) {
            Set<Integer> values = intIndexes.get(key);
            if (values == null) {
                values = new HashSet<Integer>();
            }
            values.add(value);
            intIndexes.put(key, values);
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
    public RiakIndexes addIntSet(String index, Set<Integer> newValues) {
        final IntIndex key = IntIndex.named(index);
        final String lockName = key.getFullname();
        // even though it is a concurrent hashmap, we need
        // fine grained access control for the
        // key's value set
        synchronized (lockMap.getLock(lockName)) {

            Set<Integer> values = intIndexes.get(key);

            if (values == null) {
                values = new HashSet<Integer>();
            }

            values.addAll(newValues);
            intIndexes.put(key, values);
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
