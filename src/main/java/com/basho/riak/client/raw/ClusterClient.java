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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.NodeStats;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.raw.query.indexes.IndexQuery;

/**
 * A {@link RawClient} that can be configured with a cluster of Riak clients
 * that connect to different Riak nodes.
 * 
 * By default it uses a very basic modulus round robin algorithm to select the 
 * client to use. This behavior can be modified by supplying a different
 * {@link DelegateProvider} in the {@link ClusterConfig}
 * 
 * @author russell
 * 
 */
public abstract class ClusterClient<T extends Configuration> implements RawClient {

    private final DelegateProvider delegateProvider;

    public ClusterClient(ClusterConfig<T> clusterConfig) throws IOException {
        delegateProvider = clusterConfig.getDelegateProvider();
        
        delegateProvider.init(fromConfig(clusterConfig));
        
    }

    /**
     * Create an array of clients for the cluster from the given
     * {@link ClusterConfig}.
     * 
     * @return the array of {@link RawClient} delegates that make up the cluster
     */
    protected abstract RawClient[] fromConfig(ClusterConfig<T> clusterConfig) throws IOException;

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#head(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse head(String bucket, String key, FetchMeta fetchMeta) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().head(bucket, key, fetchMeta);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse fetch(String bucket, String key) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().fetch(bucket, key);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String, int)
     */
    public RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().fetch(bucket, key, readQuorum);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String, java.lang.String, com.basho.riak.client.raw.FetchMeta)
     */
    public RiakResponse fetch(String bucket, String key, FetchMeta fetchMeta) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().fetch(bucket, key, fetchMeta);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(IRiakObject object, StoreMeta storeMeta) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().store(object, storeMeta);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * )
     */
    public void store(IRiakObject object) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().store(object);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String)
     */
    public void delete(String bucket, String key) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().delete(bucket, key);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, int)
     */
    public void delete(String bucket, String key, int deleteQuorum) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().delete(bucket, key, deleteQuorum);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, com.basho.riak.client.raw.DeleteMeta)
     */
    public void delete(String bucket, String key, DeleteMeta deleteMeta) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().delete(bucket, key, deleteMeta);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listBuckets()
     */
    public Set<String> listBuckets() throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().listBuckets();
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetchBucket(java.lang.String)
     */
    public BucketProperties fetchBucket(String bucketName) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().fetchBucket(bucketName);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#updateBucket(java.lang.String,
     * com.basho.riak.client.bucket.BucketProperties)
     */
    public void updateBucket(String name, BucketProperties bucketProperties) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().updateBucket(name, bucketProperties);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listKeys(java.lang.String)
     */
    public Iterable<String> listKeys(String bucketName) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().listKeys(bucketName);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.client.raw
     * .query.LinkWalkSpec)
     */
    public WalkResult linkWalk(LinkWalkSpec linkWalkSpec) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().linkWalk(linkWalkSpec);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.client.raw
     * .query.MapReduceSpec)
     */
    public MapReduceResult mapReduce(MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().mapReduce(spec);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().generateAndSetClientId();
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#setClientId(byte[])
     */
    public void setClientId(byte[] clientId) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().setClientId(clientId);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().getClientId();
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#ping()
     */
    public void ping() throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            delegate.getClient().ping();
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.query.IndexQuery)
     */
    public List<String> fetchIndex(IndexQuery indexQuery) throws IOException {
        final DelegateWrapper delegate = delegateProvider.getDelegate();
        try {
            return delegate.getClient().fetchIndex(indexQuery);
        } catch (IOException e) {
            delegateProvider.markAsBad(delegate, e);
            throw(e);
        }
        
    }

    public void shutdown(){
        for(DelegateWrapper dw : delegateProvider.getAllDelegates()){
            dw.getClient().shutdown();
        }
        delegateProvider.stop();
    }
    
    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.RawClient#stats()
     */
    public NodeStats stats() throws IOException {
        NodeStats nodeStats = null;
        for(DelegateWrapper dw : delegateProvider.getAllDelegates()) {
            try {
                if (nodeStats == null)
                    nodeStats = dw.getClient().stats();
                else
                    nodeStats.add(dw.getClient().stats());
            } catch (IOException e) {
                // no-op
            }
        }
        return nodeStats;
    }

}
