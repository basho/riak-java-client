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
package com.basho.riak.newapi.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.client.raw.Command;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.RiakRetryFailedException;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.DefaultRetrier;
import com.basho.riak.newapi.cap.Mutation;
import com.basho.riak.newapi.cap.UnresolvedConflictException;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.convert.Converter;

/**
 * Stores a given object into riak. Fetches first.
 * 
 * @TODO figure out if you *should* fetch first, and if you should, what about
 *       R?
 * @author russell
 * 
 */
public class StoreObject<T> implements RiakOperation<T> {

    private final RawClient client;
    private final Bucket bucket;

    // TODO populate
    private Integer r;
    private Integer w;
    private Integer dw;
    private boolean returnBody = false;

    private int retries = 0;
    private Mutation<T> mutation;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    private final String key;

    public StoreObject(final RawClient client, Bucket bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * @return null if returnBody is false
     * @throws RiakException
     */
    public T execute() throws RiakRetryFailedException, UnresolvedConflictException, ConversionException {
        // fetch, mutate, put
        Command<RiakResponse> command = new Command<RiakResponse>() {
            public RiakResponse execute() throws IOException {
                if (r != null) {
                    return client.fetch(bucket, key, r);
                } else {
                    return client.fetch(bucket, key);
                }
            }
        };

        final RiakResponse ros = new DefaultRetrier().attempt(command, retries);
        final Collection<T> siblings = new ArrayList<T>(ros.numberOfValues());

        for (RiakObject o : ros) {
            siblings.add(converter.toDomain(o));
        }

        final T resolved = resolver.resolve(siblings);
        final T mutated = mutation.apply(resolved);
        final RiakObject o = converter.fromDomain(mutated, ros.getVclock());

        final RiakResponse stored = new DefaultRetrier().attempt(new Command<RiakResponse>() {
            public RiakResponse execute() throws IOException {
                return client.store(o, generateStoreMeta());
            }
        }, retries);

        final Collection<T> storedSiblings = new ArrayList<T>(stored.numberOfValues());

        for (RiakObject s : stored) {
            storedSiblings.add(converter.toDomain(s));
        }

        return resolver.resolve(storedSiblings);
    }

    /**
     * @return
     */
    private StoreMeta generateStoreMeta() {
        return new StoreMeta(w, dw, returnBody);
    }

    public StoreObject<T> w(Integer w) {
        this.w = w;
        return this;
    }

    public StoreObject<T> dw(Integer dw) {
        this.dw = dw;
        return this;
    }

    public StoreObject<T> returnBody(boolean returnBody) {
        this.returnBody = returnBody;
        return this;
    }

    public StoreObject<T> retry(int times) {
        this.retries = times;
        return this;
    }

    public StoreObject<T> withMutator(Mutation<T> mutation) {
        this.mutation = mutation;
        return this;
    }

    public StoreObject<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    public StoreObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        return this;
    }

    /**
     * default clobber mutator. Beware.
     * 
     * @param value
     *            new value
     * @return this StoreObject
     */
    public StoreObject<T> withValue(final T value) {
        this.mutation = new Mutation<T>() {
            public T apply(T in) {
                return value;
            }
        };
        return this;
    }
}
