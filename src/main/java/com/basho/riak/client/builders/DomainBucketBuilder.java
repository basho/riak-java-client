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
package com.basho.riak.client.builders;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.ClobberMutation;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.MutationProducer;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.JSONConverter;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.StoreMeta;

/**
 * For creating a {@link DomainBucket}
 * 
 * <p>
 * Defaults are as follows:
 * <ul>
 * <li> {@link ConflictResolver} : {@link DefaultResolver} </li>
 * <li> {@link Converter} : {@link JSONConverter} </li>
 * <li> {@link Retrier} : {@link DefaultRetrier#attempts(int)} configured for 3 attempts </li>
 * <li> {@link MutationProducer} : anonymous instance that produces a {@link ClobberMutation} for {@link MutationProducer#produce(Object)}</li>
 * </p>
 * @author russell
 * @param <T>
 *            the type of the DomainBucket to be built
 */
public class DomainBucketBuilder<T> {

    private final Bucket bucket;
    private final Class<T> clazz;

    // The default resolver, it doesn't resolve
    private ConflictResolver<T> resolver = new DefaultResolver<T>();
    private Converter<T> converter;
    private Mutation<T> mutation;
    private MutationProducer<T> mutationProducer;
    private Retrier retrier = DefaultRetrier.attempts(3);
    private boolean withoutFetch;

    private FetchMeta.Builder fetchMetaBuilder = new FetchMeta.Builder();
    private StoreMeta.Builder storeMetaBuilder = new StoreMeta.Builder();
    private DeleteMeta.Builder deleteMetaBuilder = new DeleteMeta.Builder();
    private boolean returnBody = false;

    /**
     * Create a {@link DomainBucket} that stores instance of <code>clazz</code> in <code>bucket</code>
     * @param bucket
     * @param clazz
     */
    public DomainBucketBuilder(Bucket bucket, Class<T> clazz) {
        this.bucket = bucket;
        this.clazz = clazz;
        // create a default converter (the JSONConverter)
        converter = new JSONConverter<T>(clazz, bucket.getName());
    }

    /**
     * Generate the {@link DomainBucket}
     * @return a {@link DomainBucket} configured from this builder
     */
    public DomainBucket<T> build() {
        // if there is no Mutation or MutationProducer create a default one.
        if (mutation != null && mutationProducer == null) {
            mutationProducer = new MutationProducer<T>() {
                public Mutation<T> produce(T o) {
                    return mutation;
                }
            };
        } else if (mutation == null && mutationProducer == null) {
            mutationProducer = new MutationProducer<T>() {

                public Mutation<T> produce(T o) {
                    return new ClobberMutation<T>(o);
                }
            };
        }

        return new DomainBucket<T>(bucket, resolver, converter, mutationProducer,
                    storeMetaBuilder.returnBody(returnBody).build(), fetchMetaBuilder.build(), deleteMetaBuilder.build(),
                    clazz, retrier, withoutFetch);
    }

    /**
     * the {@link ConflictResolver} the {@link DomainBucket} will use.
     * @param resolver a {@link ConflictResolver}
     * @return this
     */
    public DomainBucketBuilder<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    /**
     * Should store operations on the built {@link DomainBucket} return a body?
     * @param returnBody
     * @return this
     */
    public DomainBucketBuilder<T> returnBody(boolean returnBody) {
        this.returnBody = returnBody;
        return this;
    }

    /**
     * The {@link Retrier} to use on operations on the built {@link DomainBucket}
     * @param retrier a {@link Retrier}
     * @return this
     */
    public DomainBucketBuilder<T> retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }

    /**
     * The write quorum for store operations on the built {@link DomainBucket}
     * @param w
     * @return this
     */
    public DomainBucketBuilder<T> w(int w) {
        storeMetaBuilder.w(w);
        deleteMetaBuilder.w(w);
        return this;
    }

    /**
     * The write quorum for store operations on the built {@link DomainBucket}
     * @param w - {@link Quora} to use.
     * @return this
     */
    public DomainBucketBuilder<T> w(Quora w) {
        storeMetaBuilder.w(w);
        deleteMetaBuilder.w(w);
        return this;
    }
    
    /**
     * The read quorum for fetch/store operations on the built {@link DomainBucket}
     * @param r
     * @return this
     */
    public DomainBucketBuilder<T> r(int r) {
        fetchMetaBuilder.r(r);
        deleteMetaBuilder.r(r);
        return this;
    }

    /**
     * The read quorum for fetch/store operations on the built {@link DomainBucket}
     * @param r - {@link Quora} to use
     * @return this
     */
    public DomainBucketBuilder<T> r(Quora r) {
        fetchMetaBuilder.r(r);
        deleteMetaBuilder.r(r);
        return this;
    }
    
    /**
     *The read write quorum for delete operations on the built {@link DomainBucket}
     * @param rw
     * @return this
     */
    public DomainBucketBuilder<T> rw(int rw) {
        deleteMetaBuilder.rw(rw);
        return this;
    }

    /**
     *The read write quorum for delete operations on the built {@link DomainBucket}
     * @param rw - {@link Quora} to use
     * @return this
     */
    public DomainBucketBuilder<T> rw(Quora rw) {
        deleteMetaBuilder.rw(rw);
        return this;
    }
    
    /**
     * The durable write quorum for store operations on the built {@link DomainBucket}
     * @param dw
     * @return this
     */
    public DomainBucketBuilder<T> dw(int dw) {
        storeMetaBuilder.dw(dw);
        deleteMetaBuilder.dw(dw);
        return this;
    }
    
    /**
     * The durable write quorum for store operations on the built {@link DomainBucket}
     * @param dw - {@link Quora} to use
     * @return this
     */
    public DomainBucketBuilder<T> dw(Quora dw) {
        storeMetaBuilder.dw(dw);
        deleteMetaBuilder.dw(dw);
        return this;
    }
    
    /**
     * @param notFoundOK
     * @return this
     */
    public DomainBucketBuilder<T> notFoundOK(boolean notFoundOK) {
        fetchMetaBuilder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * @param basicQuorum
     * @return this
     */
    public DomainBucketBuilder<T> basicQuorum(boolean basicQuorum) {
        fetchMetaBuilder.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * @param returnDeletedVClock
     * @return this
     */
    public DomainBucketBuilder<T> returnDeletedVClock(boolean returnDeletedVClock) {
        fetchMetaBuilder.returnDeletedVClock(returnDeletedVClock);
        return this;
    }

    /**
     * @param ifNotModified
     * @return this
     */
    public DomainBucketBuilder<T> ifNotModified(boolean ifNotModified) {
        storeMetaBuilder.ifNotModified(ifNotModified);
        return this;
    }

    /**
     * @param ifNoneMatch
     * @return this
     */
    public DomainBucketBuilder<T> ifNoneMatch(boolean ifNoneMatch) {
        storeMetaBuilder.ifNoneMatch(ifNoneMatch);
        return this;
    }

    /**
     * @param pr
     * @return this
     */
    public DomainBucketBuilder<T> pr(int pr) {
        deleteMetaBuilder.pr(pr);
        fetchMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param pr - {@link Quora} to use
     * @return this
     */
    public DomainBucketBuilder<T> pr(Quora pr) {
        deleteMetaBuilder.pr(pr);
        fetchMetaBuilder.pr(pr);
        return this;
    }
    
    /**
     * @param pw
     * @return this
     */
    public DomainBucketBuilder<T> pw(int pw) {
        deleteMetaBuilder.pw(pw);
        storeMetaBuilder.pw(pw);
        return this;
    }

    /**
     * @param pw - {@link Quora} to use
     * @return this
     */
    public DomainBucketBuilder<T> pw(Quora pw) {
        deleteMetaBuilder.pw(pw);
        storeMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * Set an operation timeout in milliseconds to be sent to Riak
     * 
     * As of 1.4 Riak allows a timeout to be sent for get, put, and delete operations. 
     * The client will receive a timeout error if the operation is not completed 
     * within the specified time
     * 
     * This is applied to all operations performed by this DomainBucket.
     * 
     * @param timeout - the timeout in milliseconds
     * @return this
     */
    public DomainBucketBuilder<T> timeout(int timeout) {
        deleteMetaBuilder.timeout(timeout);
        storeMetaBuilder.timeout(timeout);
        fetchMetaBuilder.timeout(timeout);
        return this;
    }
    
    /**
     * A {@link MutationProducer} to provide the {@link Mutation} to use in store operations.
     * @param mutationProducer
     * @return this
     */
    public DomainBucketBuilder<T> mutationProducer(MutationProducer<T> mutationProducer) {
        this.mutationProducer = mutationProducer;
        return this;
    }

    /**
     * The {@link Converter} to use on fetch and store operations on the built {@link DomainBucket}
     * @param converter
     * @return this
     */
    public DomainBucketBuilder<T> withConverter(final Converter<T> converter) {
        this.converter = converter;
        return this;
    }
    
    /**
     * Sets whether a store operation should fetch existing value(s) from Riak 
     * (and the vector clock) and perform conflict resolution if required.
     * 
     * Note this should only be used if you understand the ramifications. 
     * @see com.basho.riak.client.operations.StoreObject#withoutFetch() 
     * @param withoutFetch
     * @return this
     */
    public DomainBucketBuilder<T> withoutFetch(boolean withoutFetch) {
        this.withoutFetch = withoutFetch;
        return this;
    }
    
}
