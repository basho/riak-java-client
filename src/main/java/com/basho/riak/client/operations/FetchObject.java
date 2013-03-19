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
import java.util.Date;
import java.util.concurrent.Callable;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.*;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.FetchMeta.Builder;
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
    private RiakResponse rawResponse;

    private Retrier retrier;

    private FetchMeta.Builder builder = new Builder();

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
        final FetchMeta fetchMeta = builder.build();
        Callable<RiakResponse> command = new Callable<RiakResponse>() {
            public RiakResponse call() throws Exception {
                return client.fetch(bucket, key, fetchMeta);
            }
        };

        rawResponse = retrier.attempt(command);
        final Collection<T> siblings = new ArrayList<T>(rawResponse.numberOfValues());
        
        // When talking about tombstones, our two protocols have 
        // different behaviors. 
        //
        // When using Protocol Buffers Riak will not return a deleted vclock 
        // object unless explicitly told to do so in the request. If only
        // 
        // HTTP has no such request header/parameter and will always return 
        // the vclock of a deleted item or sibling. 
        // 
        // Unfortunately due to how the orig. HTTP client is designed it would 
        // take significant effot to rewrite it not to return the vclock from a 404 
        // back up to here based on the request meta, 
        // so we simply explicitly check it here now that we have an object(s)

        for (IRiakObject o : rawResponse) {
            if (o.isDeleted() && (fetchMeta.getReturnDeletedVClock() == null || 
                !fetchMeta.getReturnDeletedVClock()) ) {
                continue;
            }
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
    public FetchObject<T> r(int r) {
        builder.r(r);
        return this;
    }
    
    /**
     * The read quorum for this fetch operation
     * @param r an Quora for the read quorum
     * @return this
     */
    public FetchObject<T> r(Quora r) {
        builder.r(r);
        return this;
    }
    
    /**
     * The read quorum for this fetch operation
     * @param r an Quorum for the read quorum
     * @return this
     */
    public FetchObject<T> r(Quorum r) {
        builder.r(r);
        return this;
    }
    
    /**
     * @param pr
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(int)
     */
    public FetchObject<T> pr(int pr) {
        builder.pr(pr);
        return this;
    }

    /**
     * @param pr
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(Quora)
     */
    
    public FetchObject<T> pr(Quora pr) {
        builder.pr(pr);
        return this;
    }
    
    /**
     * @param pr
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(Quora)
     */
    
    public FetchObject<T> pr(Quorum pr) {
        builder.pr(pr);
        return this;
    }
    
    /**
     * @param notFoundOK
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#notFoundOK(boolean)
     */
    public FetchObject<T> notFoundOK(boolean notFoundOK) {
        builder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * @param basicQuorum
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#basicQuorum(boolean)
     */
    public FetchObject<T> basicQuorum(boolean basicQuorum) {
        builder.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * @param returnDeletedVClock
     * @return
     * @see com.basho.riak.client.raw.FetchMeta.Builder#returnDeletedVClock(boolean)
     */
    public FetchObject<T> returnDeletedVClock(boolean returnDeletedVClock) {
        builder.returnDeletedVClock(returnDeletedVClock);
        return this;
    }

    /**
     * *NOTE* HTTP Only.
     * 
     * TODO using generics and transports make this generic Transport for either
     * Date/VClock
     * 
     * @param modifiedSince
     *            a last modified date.
     * 
     * @return this
     */
    public FetchObject<T> modifiedSince(Date modifiedSince) {
        builder.modifiedSince(modifiedSince);
        return this;
    }

    /**
     * *NOTE* PB Only.
     * 
     * TODO using generics and transports make this generic T for either
     * Date/VClock
     * 
     * @param vclock
     *            a vclock
     * 
     * @return this
     */
    public FetchObject<T> ifModified(VClock vclock) {
        builder.vclock(vclock);
        return this;
    }

    /**
     * Causes the client to retrieve only the metadata and not the value
     * of this object. 
     * 
     * Note if you are using HTTP If siblings are present the client 
     * does a second get and retrieves all the values. This is due to how 
     * the HTTP API handles siblings. 
     * 
     * Note: The {@link Converter} being used must be able to handle an empty
     * value. 
     * 
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#headOnly(boolean headOnly)
     */
    public FetchObject<T> headOnly() {
        builder.headOnly(true);
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
    public FetchObject<T> withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }

    // Meta information from the raw response

    /**
     * @return true if the fetch was conditional and no result was returned
     *         since the value was unmodified
     */
    public boolean isUnmodified() {
        validatePostExecute();
        return rawResponse.isUnmodified();
    }

    /**
     * @return true if the fetch was to a deleted key but the
     *         returnDeletedVClock parameter was set, and the response has that
     *         vclock
     * 
     * @deprecated 
     * @see {@link IRiakObject#isDeleted() }
     */
    @Deprecated
    public boolean hasDeletedVclock() {
        validatePostExecute();
        return rawResponse.isDeleted();
    }

    /**
     * @return checks that the response had a vclock (i.e. was some kind of
     *         success)
     */
    public boolean hasVclock() {
        validatePostExecute();
        return rawResponse.getVclock() != null;
    }

    /**
     * @return if hasDeletedVclock or hasVclock return true, this method returns
     *         the vclock
     */
    public VClock getVClock() {
        validatePostExecute();
        return rawResponse.getVclock();
    }

    /**
     * If the {@link RiakResponse} isn't populated then the request hasn't been
     * executed.
     */
    private void validatePostExecute() {
        if (rawResponse == null) {
            throw new IllegalStateException("Please execute the operation before accessing the results");
        }
    }
}
