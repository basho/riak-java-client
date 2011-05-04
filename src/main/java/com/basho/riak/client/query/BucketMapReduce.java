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

import com.basho.riak.client.query.filter.KeyFilter;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class BucketMapReduce extends MapReduce implements Iterable<KeyFilter> {

    private final String bucket;
    private final Object keyFiltersLock = new Object();
    private final Collection<KeyFilter> keyFilters;

    public BucketMapReduce(final RawClient client, String bucket) {
        super(client);
        this.bucket = bucket;
        this.keyFilters = new LinkedList<KeyFilter>();
    }

    public BucketMapReduce(final RawClient client, String bucket, KeyFilter keyFilter) {
        super(client);
        this.bucket = bucket;
        this.keyFilters = new LinkedList<KeyFilter>();
        this.keyFilters.add(keyFilter);
    }

    public BucketMapReduce(final RawClient client, String bucket, Collection<KeyFilter> keyFilters) {
        super(client);
        this.bucket = bucket;
        this.keyFilters = new LinkedList<KeyFilter>(keyFilters);
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Copy iterator. Does not read or write through to internal BucketInput
     * state.
     */
    public Iterator<KeyFilter> iterator() {
        final Collection<KeyFilter> copyFilters = new LinkedList<KeyFilter>();

        synchronized (keyFiltersLock) {
            copyFilters.addAll(keyFilters);
        }

        return copyFilters.iterator();
    }

    public BucketMapReduce addKeyFilters(KeyFilter... keyFilters) {
        final Collection<KeyFilter> filters = Arrays.asList(keyFilters);

        synchronized (keyFiltersLock) {
            this.keyFilters.addAll(filters);
        }

        return this;
    }

    public BucketMapReduce addKeyFilter(KeyFilter keyFilter) {
        synchronized (keyFiltersLock) {
            this.keyFilters.add(keyFilter);
        }

        return this;
    }

    /**
     * @return
     */
    private boolean hasFilters() {
        synchronized (keyFiltersLock) {
            return !keyFilters.isEmpty();
        }
    }

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
