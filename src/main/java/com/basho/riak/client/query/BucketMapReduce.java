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
package com.basho.riak.client.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.query.filter.KeyFilter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.util.UnmodifiableIterator;

/**
 * Map/Reduce over a bucket, optionally add Key Filters to narrow the inputs.
 * @author russell
 * 
 * @see IRiakClient#mapReduce(String)
 */
public class BucketMapReduce extends MapReduce implements Iterable<KeyFilter> {

    private final String bucket;
    private final Object keyFiltersLock = new Object();
    private final Collection<KeyFilter> keyFilters;

    /**
     * Create a Map Reduce job over the specified <code>bucket</code> to be executed by the specified {@link RawClient}.
     * 
     * Use {@link IRiakClient#mapReduce(String)} to create your BucketMapReduce
     * @param client the {@link RawClient} to use
     * @param bucket the input to the M/R job
     */
    public BucketMapReduce(final RawClient client, String bucket) {
        super(client);
        this.bucket = bucket;
        this.keyFilters = new LinkedList<KeyFilter>();
    }

    /**
     * Get the bucket input for the M/R job
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Unmodifiable copy iterator view of the list of {@link KeyFilter}s for
     * this M/R operation. Does not read or write through to internal
     * BucketInput state.
     */
    public Iterator<KeyFilter> iterator() {
        final Collection<KeyFilter> copyFilters = new LinkedList<KeyFilter>();

        synchronized (keyFiltersLock) {
            copyFilters.addAll(keyFilters);
        }

        return new UnmodifiableIterator<KeyFilter>( copyFilters.iterator() );
    }

    /**
     * Add one or some {@link KeyFilter}s to this map/reduce operations inputs
     * @param keyFilters var arg of {@link KeyFilter}s
     * @return this
     */
    public BucketMapReduce addKeyFilters(KeyFilter... keyFilters) {
        final Collection<KeyFilter> filters = Arrays.asList(keyFilters);

        synchronized (keyFiltersLock) {
            this.keyFilters.addAll(filters);
        }

        return this;
    }

    /**
     * Add a {@link KeyFilter} to the inputs for the Map/Reduce job
     * @param keyFilter the {@link KeyFilter} to add
     * @return this
     */
    public BucketMapReduce addKeyFilter(KeyFilter keyFilter) {
        synchronized (keyFiltersLock) {
            this.keyFilters.add(keyFilter);
        }

        return this;
    }

    /**
     * Are there any key filters in the inputs to this Map/reduce operation.
     * 
     * @return true if there are any {@link KeyFilter}s, false otherwise
     */
    private boolean hasFilters() {
        synchronized (keyFiltersLock) {
            return !keyFilters.isEmpty();
        }
    }

    /**
     * Create a collection of {@link KeyFilter}s as Object[]s
     * @return the {@link KeyFilter}s copied into a collection as Object[] arrays
     */
    private Collection<Object[]> getKeyFilters() {
        final Collection<Object[]> filters = new LinkedList<Object[]>();

        for (KeyFilter filter : this) {
            filters.add(filter.asArray());
        }

        return filters;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.newapi.query.MapReduce#writeInput(org.codehaus.jackson
     * .JsonGenerator)
     */
    @Override protected void writeInput(JsonGenerator jsonGenerator) throws IOException {
        if (hasFilters()) {
            jsonGenerator.writeObject(new Object() {
                @SuppressWarnings("unused") @JsonProperty String bucket = getBucket();
                @SuppressWarnings("unused") @JsonProperty Collection<Object[]> key_filters = getKeyFilters();
            });

        } else {
            jsonGenerator.writeString(bucket);
        }
    }
}
