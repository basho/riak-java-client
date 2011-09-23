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
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;

/**
 * An operation to get some data from Riak.
 * <p>
 * Use {@link Bucket#fetch(String)} methods to create a fetch operation. Also
 * look at {@link DomainBucket#fetch(Object)}.
 * </p>
 * 
 * @author russell
 * @see Bucket
 * @see DomainBucket
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
     * Create a new FetchOperation that delegates to the given
     * <code>client</code> to fetch the data from <code>bucket</code> at
     * <code>key</code> using <code>retrier</code>
     * <p>
     * Use {@link Bucket} to create a Fetch operation, also consider using
     * {@link DomainBucket}
     * 
     * @param client
     *            the {@link RawClient} to use for the operation
     * @param bucket
     *            the name of the bucket to get the data item from
     * @param key
     *            the name of the key to get the data item from
     * @param retrier
     *            the {@link Retrier} to use when executing the operation.
     */
    public FetchObject(final RawClient client, final String bucket, final String key, final Retrier retrier) {
        this.bucket = bucket;
        this.client = client;
        this.key = key;
        this.retrier = retrier;
    }

    /**
     * Attempts to fetch the data at <code>bucket/key</code>, convert it with
     * {@link Converter} and resolve any siblings with {@link ConflictResolver}
     * 
     * @return an instance of<code>T</code> that was stored at
     *         <code>bucket/key</code> or null if not found.
     * @throws UnresolvedConflictException
     *             if the {@link ConflictResolver} used cannot get a single
     *             value from any siblings
     * @throws RiakRetryFailedException
     *             if the {@link Retrier} fails to execute the operation beyond
     *             some internal bound
     * @throws ConversionException
     *             if the supplied {@link Converter} throws trying to convert
     *             the retrieved value.
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

    /**
     * The read quorum for this fetch operation
     * @param r an Integer for the read quorum
     * @return this
     */
    public FetchObject<T> r(Integer r) {
        this.r = r;
        return this;
    }

    /**
     * A {@link Converter} to use to convert the data fetched to some other type
     * @param converter
     * @return this
     */
    public FetchObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        return this;
    }

    /**
     * A {@link Retrier} to use
     * @param retrier
     * @return
     */
    public FetchObject<T> retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
}
