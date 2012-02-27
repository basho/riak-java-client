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
package com.basho.riak.client.bucket;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.raw.RawClient;

/**
 *
 * A lazy loading BucketProperties. Defers the {@link RawClient#fetchBucket(java.lang.String) }
 * call until one of the getters is called. See {@link FetchBucket#lazyLoadBucketProperties() }
 * or {@link WriteBucket#lazyLoadBucketProperties() }
 *
 * 
 * @author roach
 * @since 1.0.4
 */
public class LazyBucketProperties implements BucketProperties {

    private final FutureTask<BucketProperties> future;

    /**
     * 
     * @param client - a {@link RawClient} to be used to fetch the bucket properties
     * @param retrier - the {@link Retrier} to use
     * @param bucketName - Name of the Riak bucket 
     */
    public LazyBucketProperties(final RawClient client, final Retrier retrier, final String bucket) {
        future = new FutureTask<BucketProperties>(new Callable<BucketProperties>() {
            public BucketProperties call() throws Exception {
                return retrier.attempt(new Callable<BucketProperties>() {
                    public BucketProperties call() throws Exception {
                        return client.fetchBucket(bucket);
                    }
                });
            }
        });
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getAllowSiblings()
     */
    public Boolean getAllowSiblings() {
        return getProperties().getAllowSiblings();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getLastWriteWins()
     */
    public Boolean getLastWriteWins() {
        return getProperties().getLastWriteWins();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getNVal()
     */
    public Integer getNVal() {
        return getProperties().getNVal();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getBackend()
     */
    public String getBackend() {
        return getProperties().getBackend();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getSmallVClock()
     */
    public Integer getSmallVClock() {
        return getProperties().getSmallVClock();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getBigVClock()
     */
    public Integer getBigVClock() {
        return getProperties().getBigVClock();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getYoungVClock()
     */
    public Long getYoungVClock() {
        return getProperties().getYoungVClock();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getOldVClock()
     */
    public Long getOldVClock() {
        return getProperties().getOldVClock();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getPrecommitHooks()
     */
    public Collection<NamedFunction> getPrecommitHooks() {
        return getProperties().getPrecommitHooks();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getPostcommitHooks()
     */
    public Collection<NamedErlangFunction> getPostcommitHooks() {
        return getProperties().getPostcommitHooks();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getR()
     */
    public Quorum getR() {
        return getProperties().getR();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getW()
     */
    public Quorum getW() {
        return getProperties().getW();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getRW()
     */
    public Quorum getRW() {
        return getProperties().getRW();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getDW()
     */
    public Quorum getDW() {
        return getProperties().getDW();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getPR()
     */
    public Quorum getPR() {
        return getProperties().getPR();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getPW()
     */
    public Quorum getPW() {
        return getProperties().getPW();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getBasicQuorum()
     */
    public Boolean getBasicQuorum() {
        return getProperties().getBasicQuorum();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getNotFoundOK()
     */
    public Boolean getNotFoundOK() {
        return getProperties().getNotFoundOK();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getChashKeyFunction()
     */
    public NamedErlangFunction getChashKeyFunction() {
        return getProperties().getChashKeyFunction();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getLinkWalkFunction()
     */
    public NamedErlangFunction getLinkWalkFunction() {
        return getProperties().getLinkWalkFunction();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#getSearch()
     */
    public Boolean getSearch() {
        return getProperties().getSearch();
    }

    /**
     * @return
     * @see com.basho.riak.client.bucket.BucketProperties#isSearchEnabled()
     */
    public boolean isSearchEnabled() {
        return getProperties().isSearchEnabled();
    }

    private BucketProperties getProperties() {
        // FutureTask has an internal state that will only allow it to 
        // actually run once.
        future.run();
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}

