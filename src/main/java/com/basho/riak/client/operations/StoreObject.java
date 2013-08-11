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
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.*;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.VClockUtil;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;

/**
 * Stores a given object into riak always fetches first.
 * 
 * <p>
 * Use {@link Bucket#store(Object)} methods to create a store operation. Also
 * look at {@link DomainBucket#store(Object)}.
 * </p>
 * 
 * TODO Should fetch first be optional? What about the vclock if not?
 * 
 * @author russell
 * @see Bucket
 * @see DomainBucket
 */
public class StoreObject<T> implements RiakOperation<T> {

    private final RawClient client;
    private final FetchObject<T> fetchObject;

    private Retrier retrier;

    private final StoreMeta.Builder storeMetaBuilder = new StoreMeta.Builder();
    private final boolean hasKey;
    
    private boolean returnBody = false;
    private boolean doNotFetch = false;
    private boolean deletedVClockWithReturnbody = false;

    private final T object;
    private Mutation<T> mutation;
    private ConflictResolver<T> resolver;
    private Converter<T> converter;

    /**
     * Create a new StoreObject operation for the object in <code>bucket</code>
     * at <code>key</code>.
     * <p>
     * Use {@link Bucket} to create a store operation.
     * </p>
     *
     * @param client
     *            the RawClient to use
     * @param bucket
     *            location of data to store
     * @param object
     *            domain object to store or mutate
     * @param key
     *            location of data to store
     * @param retrier
     *            the Retrier to use for this operation
     */
    public StoreObject(final RawClient client, final String bucket, final T object, final String key, final Retrier retrier) {
        this.object = object;
        this.client = client;
        this.retrier = retrier;
        fetchObject = new FetchObject<T>(client, bucket, key, retrier);
        hasKey = key != null;
    }

    /**
     * Fetches data from <code>bucket/key</code>, if item exists it is converted
     * with {@link Converter} and any siblings resolved with
     * {@link ConflictResolver}. {@link Mutation} is applied to the result which
     * is then converted back to {@link IRiakObject} and stored with the
     * {@link RawClient}. If <code>returnBody</code> is true then the returned
     * result is treated like a fetch (converted, conflict resolved) and the
     * resultant object returned.
     * 
     * If you wish to eliminate the fetch and conflict resolution, calling 
     * <code>withoutFetch()</code> prior to this will do so. 
     * 
     * 
     * @return the result of the store if <code>returnBody</code> is
     *         <code>true</code>, <code>null</code> if <code>returnBody</code>
     *         is <code>false</code>
     * @throws RiakException
     */
    public T execute() throws RiakRetryFailedException, UnresolvedConflictException, ConversionException {
        
        if (!hasKey && !doNotFetch) {
            throw new IllegalArgumentException("Can not store object will null key without calling withoutFetch()");
        }
        
        final T resolved;
        VClock vclock = null;
        
        if (!doNotFetch) {
            resolved = fetchObject.execute();
            vclock = fetchObject.getVClock();
        } else {
            resolved = object;
        }
        
        final T mutated = mutation.apply(resolved);
        
        if (doNotFetch) {
            vclock = VClockUtil.getVClock(mutated);
        }
        
        
        final IRiakObject o = converter.fromDomain(mutated, vclock);
        final StoreMeta storeMeta = storeMetaBuilder.returnBody(returnBody).build();

        // if non match and if not modified require extra data for the HTTP API
        // pull that from the riak object if possible
        if(storeMeta.hasIfNoneMatch() && storeMeta.getIfNoneMatch() && o != null) {
            storeMeta.etags(new String[] {o.getVtag()});
        }

        if(storeMeta.hasIfNotModified() && storeMeta.getIfNotModified()  && o != null) {
            storeMeta.lastModified(o.getLastModified());
        }

        final boolean hasMutated = !(mutation instanceof ConditionalStoreMutation<?>) || ((ConditionalStoreMutation<T>) mutation).hasMutated();
        if (hasMutated) {
            final RiakResponse stored = retrier.attempt(new Callable<RiakResponse>() {
                public RiakResponse call() throws Exception {
                    return client.store(o, storeMeta);
                }
            });
        
            final Collection<T> storedSiblings = new ArrayList<T>(stored.numberOfValues());

            // both HTTP and Protocol buffers will return tombstone siblings on a 
            // returnbody=true. There is no 'deletedvclock' option in RpbPutReq and
            // HTTP just always returns them. This makes returnbody=true consistent 
            // with a fetch operation. See: returnDeletedVClock() below

            for (IRiakObject s : stored) {
                if (s.isDeleted() && !deletedVClockWithReturnbody) {
                    continue;
                }
                storedSiblings.add(converter.toDomain(s));
            } 
            
            return resolver.resolve(storedSiblings);
            
        } else {
            return mutated;
        }
    }

    /**
     * A store performs a fetch first (to get a vclock and resolve any conflicts), set the read quorum for the fetch
     *
     * @param r the read quorum for the pre-store fetch
     * @return this
     */
    public StoreObject<T> r(int r) {
        this.fetchObject.r(r);
        return this;
    }

    /**
     * A store performs a fetch first (to get a vclock and resolve any conflicts), set the read quorum for the fetch
     *
     * @param r the read quorum for the pre-store fetch
     * @return this
     */
    public StoreObject<T> r(Quora r) {
        this.fetchObject.r(r);
        return this;
    }
    
    /**
     * A store performs a fetch first (to get a vclock and resolve any conflicts), set the read quorum for the fetch
     *
     * @param r the read quorum for the pre-store fetch
     * @return this
     */
    public StoreObject<T> r(Quorum r) {
        this.fetchObject.r(r);
        return this;
    }
    
    /**
     * The pr for the pre-store fetch
     * @param pr
     * @return
     * @see com.basho.riak.client.operations.FetchObject#pr(int)
     */
    public StoreObject<T> pr(int pr) {
        this.fetchObject.pr(pr);
        return this;
    }

    /**
     * The pr for the pre-store fetch
     * @param pr
     * @return
     * @see com.basho.riak.client.operations.FetchObject#pr(Quora)
     */
    public StoreObject<T> pr(Quora pr) {
        this.fetchObject.pr(pr);
        return this;
    }
    
    /**
     * The pr for the pre-store fetch
     * @param pr
     * @return
     * @see com.basho.riak.client.operations.FetchObject#pr(Quorum)
     */
    public StoreObject<T> pr(Quorum pr) {
        this.fetchObject.pr(pr);
        return this;
    }
    
    
    /**
     * if notfound_ok counts towards r count (for the pre-store fetch)
     * 
     * @param notFoundOK
     * @return
     * @see com.basho.riak.client.operations.FetchObject#notFoundOK(boolean)
     */
    public StoreObject<T> notFoundOK(boolean notFoundOK) {
        this.fetchObject.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * fail early if a quorum of error/notfounds are reached before a successful
     * read (for the pre-store fetch)
     * 
     * @param basicQuorum
     * @return
     * @see com.basho.riak.client.operations.FetchObject#basicQuorum(boolean)
     */
    public StoreObject<T> basicQuorum(boolean basicQuorum) {
        this.fetchObject.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * Set an operation timeout in milliseconds to be sent to Riak
     * 
     * As of 1.4 Riak allows a timeout to be sent for get, put, and delete operations. 
     * The client will receive a timeout error if the operation is not completed 
     * within the specified time
     * 
     * @param timeout
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#timeout
     */
    
    public StoreObject<T> timeout(int timeout) {
        fetchObject.timeout(timeout);
        storeMetaBuilder.timeout(timeout);
        return this;
    }
    
    /**
     * If the object has just been deleted, there maybe a tombstone value
     * vclock, set to true to have this returned in the pre-store fetch.
     * 
     * @param returnDeletedVClock
     * @return
     * @see com.basho.riak.client.operations.FetchObject#returnDeletedVClock(boolean)
     */
    public StoreObject<T> returnDeletedVClock(boolean returnDeletedVClock) {
        this.fetchObject.returnDeletedVClock(returnDeletedVClock);
        deletedVClockWithReturnbody = true;
        return this;
    }

    /**
     * Set the primary write quorum for the store operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     */
    public StoreObject<T> pw(int pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }

    /**
     * Set the primary write quorum for the store operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     */
    public StoreObject<T> pw(Quora pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * Set the primary write quorum for the store operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     */
    public StoreObject<T> pw(Quorum pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * Set the write quorum for the store operation
     * @param w
     * @return this
     */
    public StoreObject<T> w(int w) {
        storeMetaBuilder.w(w);
        return this;
    }

    /**
     * Set the write quorum for the store operation
     * @param w
     * @return this
     */
    public StoreObject<T> w(Quora w) {
        storeMetaBuilder.w(w);
        return this;
    }

/**
     * Set the write quorum for the store operation
     * @param w
     * @return this
     */
    public StoreObject<T> w(Quorum w) {
        storeMetaBuilder.w(w);
        return this;
    }


    /**
     * The durable write quorum for this store operation
     * @param dw
     * @return this
     */
    public StoreObject<T> dw(int dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * The durable write quorum for this store operation
     * @param dw
     * @return this
     */
    public StoreObject<T> dw(Quora dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * The durable write quorum for this store operation
     * @param dw
     * @return this
     */
    public StoreObject<T> dw(Quorum dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * Should the store operation return a response body?
     * @param returnBody
     * @return this
     */
    public StoreObject<T> returnBody(boolean returnBody) {
        this.returnBody = returnBody;
        return this;
    }

    /**
     * If you don't know what this is or what it does, you should not 
     * be using it. 
     * @param asis
     * @return this
     */
    public StoreObject<T> asis(boolean asis) {
        storeMetaBuilder.asis(asis);
        return this;
    }
    
    /**
     * Default is false (i.e. NOT a conditional store).
     * <p>
     * NOTE: This has different meanings depending on the underlying transport.
     * </p>
     * <p>
     * In the case of the PB interface it means: Only store if there is no
     * bucket/key entry for this object in the database already.
     * </p>
     * <p>
     * For the HTTP API it means: Only store if there is no entity that matches
     * some etags I provide you
     * </p>
     * <p>
     * To make this transparent the StoreOperation will pull the etag from the
     * object returned in the pre-store fetch, and use that as supplementary
     * data to the HTTP Store request.
     * </p>
     * <p>
     * If there is match (b/k or etag) then the operation is *not* retried by
     * the retrier, to override this, provide a custom retrier.
     * </p>
     * 
     * @param ifNoneMatch
     *            true if you want a conditional store, false otherwise,
     *            defaults to false.
     * @return this
     */
    public StoreObject<T> ifNoneMatch(boolean ifNoneMatch) {
        storeMetaBuilder.ifNoneMatch(ifNoneMatch);
        return this;
    }

    /**
     * Default is false (i.e. NOT a conditional store).
     * <p>
     * NOTE: This has different meanings depending on the underlying transport.
     * </p>
     * <p>
     * In the case of the PB interface it means: Only store if the vclock
     * provided with the store is the same as the one in Riak for this object
     * (i.e. the object has not been modified since you last got it), of course,
     * since this StoreObject does a fetch before a store the window for
     * concurrent modification is minimized, but this is an extra guard, still.
     * </p>
     * <p>
     * For the HTTP API it means: Only store if there has been no modification
     * since the provided timestamp.
     * </p>
     * <p>
     * To make this transparent the StoreOperation will pull the last
     * modified date from the object returned in the pre-store fetch, and use
     * that as supplementary data to the HTTP Store request.
     * </p>
     * 
     * @param ifNotModified
     *            true if you want a conditional store, false otherwise,
     *            defaults to false.
     * @return this
     */
    public StoreObject<T> ifNotModified(boolean ifNotModified) {
        storeMetaBuilder.ifNotModified(ifNotModified);
        return this;
    }

    /**
     * The {@link Retrier} to use for the fetch and store operations.
     * @param retrier a {@link Retrier}
     * @return this
     */
    public StoreObject<T> withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        this.fetchObject.withRetrier(retrier);
        return this;
    }

    /**
     * The {@link Mutation} to apply to the value retrieved from the fetch operation
     * @param mutation a {@link Mutation}
     * @return this
     */
    public StoreObject<T> withMutator(Mutation<T> mutation) {
        this.mutation = mutation;
        return this;
    }

    /**
     * The {@link ConflictResolver} to use on any sibling results returned from the fetch (and store if <code>returnBody</code> is true)
     * NOTE: since it is used for fetch and after store must be reusable.
     * @param resolver a {@link ConflictResolver}
     * @return this
     */
    public StoreObject<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        this.fetchObject.withResolver(resolver);
        return this;
    }

    /**
     * The {@link Converter} to use
     * @param converter a {@link Converter}
     * @return this
     */
    public StoreObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        this.fetchObject.withConverter(converter);
        return this;
    }

    /**
     * Creates a {@link ClobberMutation} that applies <code>value</code>
     * 
     * @param value
     *            new value
     * @return this StoreObject
     */
    public StoreObject<T> withValue(final T value) {
        this.mutation = new ClobberMutation<T>(value);
        return this;
    }
    
    /**
     * Eliminates fetching the existing value before storing the current one.
     * 
     * The original client design was based around the StoreObject retrieving 
     * an existing value from Riak, resolving siblings, applying a Mutation, then
     * finally storing the modified object to Riak. In several use cases this is
     * not optimal.
     * 
     * By calling this method prior to <cod>execute()</code> the fetch and 
     * conflict resolution will not occur. 
     * 
     * Care must be taken when using this option:
     * 
     * 1) <code>null</code> will be passed to the {@link Mutation} object (if 
     *    you are using the default {@link ClobberMutation} this is fine).
     * 2) A vector clock should be provided by the object returned from your
     *    {@link Mutation}. 
     * 
     */
    public StoreObject<T> withoutFetch() {
        this.doNotFetch = true;
        return this;
    }
}
