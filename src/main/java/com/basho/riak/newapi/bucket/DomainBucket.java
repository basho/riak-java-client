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
package com.basho.riak.newapi.bucket;

import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.builders.DomainBucketBuilder;
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.Mutation;
import com.basho.riak.newapi.cap.MutationProducer;
import com.basho.riak.newapi.convert.ConversionUtil;
import com.basho.riak.newapi.convert.Converter;

/**
 * A domain bucket is a wrapper around a bucket that is strongly typed uses a
 * preset resolver, mutation producer, converter, r, w, dw, rw, retries,
 * returnBody etc
 * 
 * @author russell
 * 
 */
public class DomainBucket<T> {

    private final Bucket bucket;
    private final ConflictResolver<T> resolver;
    private final Converter<T> converter;
    private final MutationProducer<T> mutationProducer;
    private final Integer w;
    private final Integer dw;
    private final Integer r;
    private final Integer rw;
    private final boolean returnBody;
    private final int retries;
    private final Class<T> clazz;

    /**
     * @param bucket
     * @param resolver
     * @param converter
     * @param mutation
     * @param w
     * @param dw
     * @param r
     * @param rw
     * @param returnBody
     * @param retries
     * @param clazz
     */
    public DomainBucket(Bucket bucket, ConflictResolver<T> resolver, Converter<T> converter,
            MutationProducer<T> mutationProducer, Integer w, Integer dw, Integer r, Integer rw, boolean returnBody,
            int retries, Class<T> clazz) {
        this.bucket = bucket;
        this.resolver = resolver;
        this.converter = converter;
        this.mutationProducer = mutationProducer;
        this.w = w;
        this.dw = dw;
        this.r = r;
        this.rw = rw;
        this.returnBody = returnBody;
        this.retries = retries;
        this.clazz = clazz;
    }

    public T store(T o) throws RiakException {
        final Mutation<T> mutation = mutationProducer.produce(o);
        return bucket.store(o).withConverter(converter).withMutator(mutation).withResolver(resolver).w(w).dw(dw).retry(retries).returnBody(returnBody).execute();
    }

    public T fetch(String key) throws RiakException {
        return bucket.fetch(key, clazz).withConverter(converter).withResolver(resolver).r(r).retry(retries).execute();
    }

    public T fetch(T o) throws RiakException {
        return bucket.fetch(o).withConverter(converter).withResolver(resolver).r(r).retry(retries).execute();
    }

    public void delete(T o) throws RiakException {
        final String key = ConversionUtil.getKey(o);
        delete(key);
    }

    public void delete(String key) throws RiakException {
        bucket.delete(key).rw(rw).execute();
    }

    /**
     * @param b
     *            the Bucket to wrap
     * @param clazz
     * @return a DomainBucketBuilder for the wrapped bucket
     */
    public static <T> DomainBucketBuilder<T> builder(Bucket b, Class<T> clazz) {
        return new DomainBucketBuilder<T>(b, clazz);
    }
}
