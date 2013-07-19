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
package com.basho.riak.client.raw;

import com.basho.riak.client.IndexEntry;
import com.basho.riak.client.query.StreamingOperation;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.NodeStats;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.query.IndexSpec;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.raw.query.indexes.IndexQuery;

/**
 * A {@link RawClient} that can be configured with a cluster of Riak clients
 * that connect to different Riak nodes.
 * 
 * It uses a very basic modulus round robin algorithm to select the client to
 * use.
 * 
 * @author russell
 * 
 */
public abstract class ClusterClient<T extends Configuration> implements RawClient {

    private final RawClient[] cluster;
    private final int clusterSize;
    private final AtomicInteger counter;

    public ClusterClient(ClusterConfig<T> clusterConfig) throws IOException {
        cluster = fromConfig(clusterConfig);
        clusterSize = cluster.length;
        counter = new AtomicInteger(0);
    }

    /**
     * Create an array of clients for the cluster from the given
     * {@link ClusterConfig}.
     * 
     * @return the array of {@link RawClient} delegates that make up the cluster
     */
    protected abstract RawClient[] fromConfig(ClusterConfig<T> clusterConfig) throws IOException;

    /**
     * Get a {@link RawClient} delegate from the array that makes up the
     * cluster, basic round robin.
     * 
     * TODO abstract this out to a strategy so users can provide alternative
     * load balancing/client selection strategies
     * 
     * @return a {@link RawClient} to be a delegate for the requested operation.
     */
    private RawClient getDelegate() {
        int delegateIndex = Math.abs(counter.getAndIncrement() % clusterSize);
        return cluster[delegateIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#head(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse head(String bucket, String key, FetchMeta fetchMeta) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.head(bucket, key, fetchMeta);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse fetch(String bucket, String key) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetch(bucket, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String, int)
     */
    public RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetch(bucket, key, readQuorum);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String, java.lang.String, com.basho.riak.client.raw.FetchMeta)
     */
    public RiakResponse fetch(String bucket, String key, FetchMeta fetchMeta) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetch(bucket, key, fetchMeta);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(IRiakObject object, StoreMeta storeMeta) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.store(object, storeMeta);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * )
     */
    public void store(IRiakObject object) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.store(object);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String)
     */
    public void delete(String bucket, String key) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.delete(bucket, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, int)
     */
    public void delete(String bucket, String key, int deleteQuorum) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.delete(bucket, key, deleteQuorum);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, com.basho.riak.client.raw.DeleteMeta)
     */
    public void delete(String bucket, String key, DeleteMeta deleteMeta) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.delete(bucket, key, deleteMeta);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listBuckets()
     */
    public Set<String> listBuckets() throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.listBuckets();
    }

    public StreamingOperation<String> listBucketsStreaming() throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.listBucketsStreaming();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetchBucket(java.lang.String)
     */
    public BucketProperties fetchBucket(String bucketName) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetchBucket(bucketName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#updateBucket(java.lang.String,
     * com.basho.riak.client.bucket.BucketProperties)
     */
    public void updateBucket(String name, BucketProperties bucketProperties) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.updateBucket(name, bucketProperties);
    }

    public void resetBucketProperties(String bucketName) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.resetBucketProperties(bucketName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listKeys(java.lang.String)
     */
    public StreamingOperation<String> listKeys(String bucketName) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.listKeys(bucketName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.client.raw
     * .query.LinkWalkSpec)
     */
    public WalkResult linkWalk(LinkWalkSpec linkWalkSpec) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.linkWalk(linkWalkSpec);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.client.raw
     * .query.MapReduceSpec)
     */
    public MapReduceResult mapReduce(MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        final RawClient delegate = getDelegate();
        return delegate.mapReduce(spec);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.generateAndSetClientId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#setClientId(byte[])
     */
    public void setClientId(byte[] clientId) throws IOException {
        final RawClient delegate = getDelegate();
        delegate.setClientId(clientId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.getClientId();
    }

    /* 
     * Pings every node in the cluster. If any node is down, will
     * throw an exception.
     */
    public void ping() throws IOException {
        for(RawClient rc : cluster) {
            rc.ping();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.query.IndexQuery)
     */
    public List<String> fetchIndex(IndexQuery indexQuery) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetchIndex(indexQuery);
    }

    public StreamingOperation<IndexEntry> fetchIndex(IndexSpec indexSpec) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetchIndex(indexSpec);
    }
    
    public Long incrementCounter(String bucket, String counter, long increment, StoreMeta meta) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.incrementCounter(bucket, counter, increment, meta);
    }
    
    public Long fetchCounter(String bucket, String counter, FetchMeta meta) throws IOException {
        final RawClient delegate = getDelegate();
        return delegate.fetchCounter(bucket, counter, meta);
    }
    
    public void shutdown(){
        for(RawClient rc : cluster){
            rc.shutdown();
        }
    }
    
    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.RawClient#stats()
     */
    public NodeStats stats() throws IOException {
        NodeStats nodeStats = null;
        for(RawClient rc : cluster) {
            if (nodeStats == null)
                nodeStats = rc.stats();
            else
                nodeStats.add(rc.stats());
        }
        return nodeStats;
    }

}
