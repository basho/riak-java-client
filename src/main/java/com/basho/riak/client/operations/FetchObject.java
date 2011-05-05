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
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;

/**
 * @author russell
 * 
 */
public class FetchObject<T> implements RiakOperation<T> {

    private final String bucket;
    private final RawClient client;
    private final String key;

    private Retrier retrier;

    private Integer r;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    /**
     * @param bucket
     * @param client
     */
    public FetchObject(final RawClient client, final String bucket, final String key, final Retrier retrier) {
        this.bucket = bucket;
        this.client = client;
        this.key = key;
        this.retrier = retrier;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public T execute() throws UnresolvedConflictException, RiakRetryFailedException, ConversionException {
        // fetch, resolve
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

        return resolver.resolve(siblings);
    }

    public FetchObject<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    public FetchObject<T> r(int r) {
        this.r = r;
        return this;
    }

    public FetchObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        return this;
    }

    public FetchObject<T> retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
}
