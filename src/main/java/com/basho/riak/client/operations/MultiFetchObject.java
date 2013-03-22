/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.operations;

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.query.MultiFetchFuture;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An operation to fetch multiple values from Riak
 * 
 * <p>
 * Use the {@link com.basho.riak.client.bucket.Bucket#multiFetch(java.lang.String[])}, 
 * {@link com.basho.riak.client.bucket.Bucket#multiFetch(java.util.List, java.lang.Class)},
 * or {@link com.basho.riak.client.bucket.Bucket#multiFetch(java.util.List, java.lang.Class)}
 * methods to create a mutli-fetch operation. 
 * </p>
 * <p>
 * Riak itself does not support pipelining of requests. The MutlFetchObject addresses
 * this issue by using a threadpool to parallelize a set of fetch operations for
 * a given set of keys.
 * </p>
 * <p>
 * <b>Thread Pool:</b><br/>
 * The internal {@link ThreadPoolExecutor} is static; all multi-fetch operations
 * performed by a single instance of the client use the same pool. This is to prevent resource 
 * starvation in the case of multiple simultaneous multi-fetch operations. Idle threads
 * (including core threads) are timed out after 5 seconds.<br/><br/>
 * The defaults for corePoolSize and maximumPoolSize are determined by the Java
 * Runtime using:<br/><br/>
 * {@code Runtime.getRuntime().availableProcessors() * 2;}
 * </p>
 * <p>
 * Advanced users can tune this via the {@link #setCorePoolSize(int)} and 
 * {@link #setMaximumPoolSize(int)} methods; these are passed directly to their 
 * counterparts in the {@link ThreadPoolExecutor}. The queue feeding the threadpool 
 * is unbounded. 
 * </p>
 * <p>
 * Be aware that because requests are being parallelized performance is also
 * dependent on the client's underlying connection pool. If there are no connections 
 * available performance will not be increased over making the requests sequentially. 
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @see com.basho.riak.client.bucket.Bucket
 * @see com.basho.riak.client.RiakFactory
 */
public class MultiFetchObject<T> implements RiakOperation<List<MultiFetchFuture<T>>>
{
    /**
     * The initial value for both corePoolSize and maximumPoolSize. This is determined via:
     * {@code Runtime.getRuntime().availableProcessors() * 2;}
     * @see ThreadPoolExecutor
     */
    public static final int DEFAULT_POOL_MAX_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    
    private static final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    private static final ThreadPoolExecutor threadPool = 
        new ThreadPoolExecutor(DEFAULT_POOL_MAX_SIZE, DEFAULT_POOL_MAX_SIZE, 5, TimeUnit.SECONDS, workQueue);
    
    static {
        threadPool.allowCoreThreadTimeOut(true);
    }
    
    private final String bucket;
    private final RawClient client;
    private final List<String> keys;
    
    private FetchMeta.Builder builder = new FetchMeta.Builder();

    private ConflictResolver<T> resolver;
    private Converter<T> converter;
    private Retrier retrier;
    
    
    /**
     * Create a new MultiFetchOperation that delegates to the given
     * <code>client</code> to fetch the data from <code>bucket</code> for
     * <code>keys</code> using <code>retrier</code>
     * <p>
     * Use {@link com.basho.riak.client.bucket.Bucket} to create a Fetch operation.
     * 
     * @param client
     *            the {@link RawClient} to use for the operation
     * @param bucket
     *            the name of the bucket to get the data item from
     * @param keys
     *            the keys to get the data items from
     * @param retrier
     *            the {@link Retrier} to use when executing the operation.
     */
    public MultiFetchObject(final RawClient client, final String bucket, final List<String> keys, final Retrier retrier)
    {
        this.bucket = bucket;
        this.client = client;
        this.keys = keys;
        this.retrier = retrier;
    }
    
    /**
     * Attempts to fetch the data for all the keys, convert it with 
     * {@link Converter} and resolve any siblings with {@link ConflictResolver}
     * 
     * @return a List of {@link MultiFetchFuture} objects. 
     */
    public List<MultiFetchFuture<T>> execute() 
    {
        List<MultiFetchFuture<T>> futureList = new ArrayList<MultiFetchFuture<T>>(keys.size());
        FetchMeta fetchMeta = builder.build();
        for (String key : keys)
        {
            FetchObject<T> fetchObject = new FetchObject<T>(client, bucket, key, retrier, fetchMeta)
                                                .withConverter(converter)
                                                .withResolver(resolver);
            
            MultiFetchCallable<T> callable = new MultiFetchCallable<T>(fetchObject);
            MultiFetchFuture<T> task = new MultiFetchFuture(key, callable);
            futureList.add(task);
            threadPool.execute(task);
        }
        
        return futureList;
        
    }

    /**
     * Sets the maximum number of threads in the internal {@link ThreadPoolExecutor}.
     * 
     * @param size - the new maximum
     * @see ThreadPoolExecutor#setMaximumPoolSize(int) 
     */
    public static void setMaximumPoolSize(int size)
    {
        threadPool.setMaximumPoolSize(size);
    }
    
    /**
     * Returns the maximum allowed number of threads from the internal {@link ThreadPoolExecutor}
     * 
     * @return the maximum allowed number of threads
     * @see ThreadPoolExecutor#getMaximumPoolSize() 
     */
    public static int getMaximumPoolSize()
    {
        return threadPool.getMaximumPoolSize();
    }
    
    /**
     * Sets the core number of threads in the internal {@link ThreadPoolExecutor}.
     * 
     * @param size - the new core size
     * @see ThreadPoolExecutor#setCorePoolSize(int) 
     */
    public static void setCorePoolSize(int size)
    {
        threadPool.setCorePoolSize(size);
    }
    
    /**
     * Returns the core number of threads from the internal {@link ThreadPoolExecutor}
     * @return the core number of threads
     * @see ThreadPoolExecutor#getCorePoolSize() 
     */
    public static int getCorePoolSize()
    {
        return threadPool.getCorePoolSize();
    }
    
    /**
     * Sets the {@link ConflictResolver} to use for this multi-fetch operation.
     * @param resolver
     * @return this
     */
    public MultiFetchObject<T> withResolver(ConflictResolver<T> resolver) {
        this.resolver = resolver;
        return this;
    }

    /**
     * The read quorum for this fetch operation
     * @param r an Integer for the read quorum
     * @return this
     */
    public MultiFetchObject<T> r(int r) {
        builder.r(r);
        return this;
    }
    
    /**
     * The read quorum for this fetch operation
     * @param r an Quora for the read quorum
     * @return this
     */
    public MultiFetchObject<T> r(Quora r) {
        builder.r(r);
        return this;
    }
    
    /**
     * The read quorum for this fetch operation
     * @param r an Quorum for the read quorum
     * @return this
     */
    public MultiFetchObject<T> r(Quorum r) {
        builder.r(r);
        return this;
    }
    
    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(int)
     */
    public MultiFetchObject<T> pr(int pr) {
        builder.pr(pr);
        return this;
    }

    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(Quora)
     */
    
    public MultiFetchObject<T> pr(Quora pr) {
        builder.pr(pr);
        return this;
    }
    
    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(Quora)
     */
    
    public MultiFetchObject<T> pr(Quorum pr) {
        builder.pr(pr);
        return this;
    }
    
    /**
     * @param notFoundOK
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#notFoundOK(boolean)
     */
    public MultiFetchObject<T> notFoundOK(boolean notFoundOK) {
        builder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * @param basicQuorum
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#basicQuorum(boolean)
     */
    public MultiFetchObject<T> basicQuorum(boolean basicQuorum) {
        builder.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * @param returnDeletedVClock
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#returnDeletedVClock(boolean)
     */
    public MultiFetchObject<T> returnDeletedVClock(boolean returnDeletedVClock) {
        builder.returnDeletedVClock(returnDeletedVClock);
        return this;
    }

    /**
     * *NOTE* HTTP Only.
     * 
     * @param modifiedSince
     *            a last modified date.
     * 
     * @return this
     */
    public MultiFetchObject<T> modifiedSince(Date modifiedSince) {
        builder.modifiedSince(modifiedSince);
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
    public MultiFetchObject<T> headOnly() {
        builder.headOnly(true);
        return this;
    }

    /**
     * A {@link Converter} to use to convert the data fetched to some other type
     * @param converter
     * @return this
     */
    public MultiFetchObject<T> withConverter(Converter<T> converter) {
        this.converter = converter;
        return this;
    }

    /**
     * A {@link Retrier} to use
     * @param retrier
     * @return this
     */
    public MultiFetchObject<T> withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }

    private class MultiFetchCallable<T> implements Callable<T>
    {
        private FetchObject<T> fetchObject;
        
        public MultiFetchCallable(FetchObject<T> fetchObject)
        {
            this.fetchObject = fetchObject;
        }
        
        public T call() throws Exception
        {
            return fetchObject.execute();
        }
        
    }
    
}

