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
package com.basho.riak.client.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;

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
    private final String bucket;

    private Retrier retrier;

    // TODO populate
    private Integer r;
    private Integer w;
    private Integer dw;
    private boolean returnBody = false;

    private Mutation<T> mutation;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    private final String key;

    public StoreObject(final RawClient client, String bucket, String key, final Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.retrier = retrier;
    }

    /**
     * @return null if returnBody is false
     * @throws RiakException
     */
    public T execute() throws RiakRetryFailedException, UnresolvedConflictException, ConversionException {
        // fetch, mutate, put
        Callable<RiakResponse> command = new Callable<RiakResponse>() {
            public RiakResponse call() throws Exception {
                if (r != null) {
                    return client.fetch(bucket, key, r);
                } else {
                    return client.fetch(bucket, key);
                }
            }
        };

        final RiakResponse ros = retrier.attempt(command);
        final Collection<T> siblings = new ArrayList<T>(ros.numberOfValues());

        for (IRiakObject o : ros) {
            siblings.add(converter.toDomain(o));
        }

        final T resolved = resolver.resolve(siblings);
        final T mutated = mutation.apply(resolved);
        final IRiakObject o = converter.fromDomain(mutated, ros.getVclock());

        final RiakResponse stored = retrier.attempt(new Callable<RiakResponse>() {
            public RiakResponse call() throws Exception {
                return client.store(o, generateStoreMeta());
            }
        });

        final Collection<T> storedSiblings = new ArrayList<T>(stored.numberOfValues());

        for (IRiakObject s : stored) {
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

    public StoreObject<T> retrier(final Retrier retrier) {
        this.retrier = retrier;
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
