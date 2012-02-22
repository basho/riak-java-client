/*
 * Copyright 2012 roach.
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
package com.basho.riak.client.bucket;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.raw.RawClient;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * A lazy loading BucketProperties. Defers the {@link RawClient#fetchBucket(java.lang.String) }
 * call until one of the getters is called. See {@link FetchBucket#lazyLoadBucketProperties() }
 * or {@link WriteBucket#lazyLoadBucketProperties() }
 * 
 * 
 * @author roach
 */

public class LazyBucketProperties implements BucketProperties
{

    private BucketProperties properties;
    private final AtomicBoolean isLoaded = new AtomicBoolean(false);
    private final CountDownLatch countdownLatch = new CountDownLatch(1);
    private final RawClient client;
    private final Retrier retrier;
    private final String bucketName;
    
    
    /**
     * 
     * @param client - a {@link RawClient} to be used to fetch the bucket properties
     * @param retrier - the {@link Retrier} to use
     * @param bucketName - Name of the Riak bucket 
     */
    public LazyBucketProperties(RawClient client, Retrier retrier, String bucketName)
    {
        this.client = client;
        this.retrier = retrier;
        this.bucketName = bucketName;
    }
    
    
    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getAllowSiblings()
     */
    public Boolean getAllowSiblings()
    {
        lazyLoad();
        return properties.getAllowSiblings();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getLastWriteWins()
     */
    public Boolean getLastWriteWins()
    {
        lazyLoad();
        return properties.getLastWriteWins();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getNVal()
     */
    public Integer getNVal()
    {
        lazyLoad();
        return properties.getNVal();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getBackend()
     */
    public String getBackend()
    {
        lazyLoad();
        return properties.getBackend();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getSmallVClock()
     */
    public Integer getSmallVClock()
    {
        lazyLoad();
        return properties.getSmallVClock();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getBigVClock()
     */
    public Integer getBigVClock()
    {
        lazyLoad();
        return properties.getBigVClock();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getYoungVClock()
     */
    public Long getYoungVClock()
    {
        lazyLoad();
        return properties.getYoungVClock();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getOldVClock()
     */
    public Long getOldVClock()
    {
        lazyLoad();
        return properties.getOldVClock();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getPrecommitHooks()
     */
    public Collection<NamedFunction> getPrecommitHooks()
    {
        lazyLoad();
        return properties.getPrecommitHooks();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getPostcommitHooks()
     */
    public Collection<NamedErlangFunction> getPostcommitHooks()
    {
        lazyLoad();
        return properties.getPostcommitHooks();
    }

    /*
     * (non-Javadoc) @see com.basho.riak.client.bucket.BucketProperties#getR()
     */
    public Quorum getR()
    {
        lazyLoad();
        return properties.getR();
    }

    /*
     * (non-Javadoc) @see com.basho.riak.client.bucket.BucketProperties#getW()
     */
    public Quorum getW()
    {
        lazyLoad();
        return properties.getW();
    }

    /*
     * (non-Javadoc) @see com.basho.riak.client.bucket.BucketProperties#getRW()
     */
    public Quorum getRW()
    {
        lazyLoad();
        return properties.getRW();
    }

    /*
     * (non-Javadoc) @see com.basho.riak.client.bucket.BucketProperties#getDW()
     */
    public Quorum getDW()
    {
        lazyLoad();
        return properties.getDW();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.bucket.BucketProperties#getPR()
     */
    public Quorum getPR()
    {
        lazyLoad();
        return properties.getPR();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.bucket.BucketProperties#getPW()
     */
    public Quorum getPW()
    {
        lazyLoad();
        return properties.getPW();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.bucket.BucketProperties#getBasicQuorum()
     */
    public Boolean getBasicQuorum()
    {
        lazyLoad();
        return properties.getBasicQuorum();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.bucket.BucketProperties#getNotFoundOK()
     */
    public Boolean getNotFoundOK()
    {
        lazyLoad();
        return properties.getNotFoundOK();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getChashKeyFunction()
     */
    public NamedErlangFunction getChashKeyFunction()
    {
        lazyLoad();
        return properties.getChashKeyFunction();
    }

    /*
     * (non-Javadoc) @see
     * com.basho.riak.client.bucket.BucketProperties#getLinkWalkFunction()
     */
    public NamedErlangFunction getLinkWalkFunction()
    {
        lazyLoad();
        return properties.getLinkWalkFunction();
    }

    public Boolean getSearch()
    {
        lazyLoad();
        return properties.getSearch();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.bucket.BucketProperties#isSearchEnabled()
     */
    public boolean isSearchEnabled()
    {
        lazyLoad();
        return properties.isSearchEnabled();
    }

    private void lazyLoad()
    {
        boolean notLoaded = isLoaded.compareAndSet(false, true);
        if (notLoaded)
        {
            try
            {
                properties = retrier.attempt(new Callable<BucketProperties>() {
                    public BucketProperties call() throws Exception {
                        return client.fetchBucket(bucketName);
                    }
                });
            }
            catch (RiakRetryFailedException ex)
            {
                // We have to reset the state here to avoid a race condition where 
                // other threads could be waiting for the latch
                isLoaded.set(false);
                throw new RuntimeException("Lazy loading of BucketProperties failed", ex);
            }
            finally
            {
                countdownLatch.countDown();
            }
        }
        else
        {
            try
            {
                countdownLatch.await();
                // This ensures that if the thread retreiving the properties fails
                // all waiting threads will also fail. 
                if (!isLoaded.get())
                {
                    throw new RuntimeException("Lazy loading of BucketProperties failed");
                }
            }
            catch (InterruptedException ex)
            {
                throw new RuntimeException("Interrupted while waiting for BucketProperties to lazy load", ex);
            }
        }
    }
}
