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
package com.basho.riak.newapi.builders;

import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.DomainBucket;
import com.basho.riak.newapi.cap.ClobberMutation;
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.DefaultResolver;
import com.basho.riak.newapi.cap.Mutation;
import com.basho.riak.newapi.cap.MutationProducer;
import com.basho.riak.newapi.convert.Converter;
import com.basho.riak.newapi.convert.JSONConverter;

/**
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

    private Integer w;
    private Integer dw;
    private Integer r;
    private Integer rw;
    private boolean returnBody = false;
    private int retries = 0;

    /**
     * @param bucket
     * @param clazz
     */
    public DomainBucketBuilder(Bucket bucket, Class<T> clazz) {
        this.bucket = bucket;
        this.clazz = clazz;
        // create a default converter (the JSONConverter)
        converter = new JSONConverter<T>(clazz, bucket.getName());
    }

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

        return new DomainBucket<T>(bucket, resolver, converter, mutationProducer, w, dw, r, rw, returnBody, retries,
                                   clazz);
    }

    /**
     * @param mergeResolver
     * @return
     */
    public DomainBucketBuilder<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    /**
     * @param returnBody
     * @return
     */
    public DomainBucketBuilder<T> returnBody(boolean returnBody) {
        this.returnBody = returnBody;
        return this;
    }

    /**
     * @param i
     * @return
     */
    public DomainBucketBuilder<T> retry(int times) {
        this.retries = times;
        return this;
    }

    /**
     * @param i
     * @return
     */
    public DomainBucketBuilder<T> w(int w) {
        this.w = w;
        return this;
    }

    public DomainBucketBuilder<T> r(int r) {
        this.r = r;
        return this;
    }

    public DomainBucketBuilder<T> rw(int rw) {
        this.rw = rw;
        return this;
    }

    public DomainBucketBuilder<T> dw(int dw) {
        this.dw = dw;
        return this;
    }

    public DomainBucketBuilder<T> mutationProducer(MutationProducer<T> mutationProducer) {
        this.mutationProducer = mutationProducer;
        return this;
    }

    public DomainBucketBuilder<T> withConverter(final Converter<T> converter) {
        this.converter = converter;
        return this;
    }
}
