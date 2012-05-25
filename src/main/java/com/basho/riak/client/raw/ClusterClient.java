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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.NodeStats;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.cluster.ClusterDelegate;
import com.basho.riak.client.raw.cluster.ClusterTask;
import com.basho.riak.client.raw.cluster.ClusterTaskException;
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
 * It uses a very basic modulus round robin algorithm to select the client to
 * use.
 *
 * @author russell
 */
public abstract class ClusterClient<T extends Configuration> implements RawClient {

    private final ClusterDelegate clusterDelegate;

    public ClusterClient(ClusterConfig<T> clusterConfig) throws IOException {
        clusterDelegate = new ClusterDelegate(Arrays.asList(fromConfig(clusterConfig)), clusterConfig);
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
    public RiakResponse head(final String bucket, final String key, final FetchMeta fetchMeta) throws IOException {
        return clusterDelegate.execute(new ClusterTask<RiakResponse>() {
            public RiakResponse call(final RawClient client) throws IOException {
                return client.head(bucket, key, fetchMeta);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse fetch(final String bucket, final String key) throws IOException {
        return clusterDelegate.execute(new ClusterTask<RiakResponse>() {
            public RiakResponse call(final RawClient client) throws IOException {
                return client.fetch(bucket, key);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String, int)
     */
    public RiakResponse fetch(final String bucket, final String key, final int readQuorum) throws IOException {
        return clusterDelegate.execute(new ClusterTask<RiakResponse>() {
            public RiakResponse call(final RawClient client) throws IOException {
                return client.fetch(bucket, key, readQuorum);
            }
        });
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String, java.lang.String, com.basho.riak.client.raw.FetchMeta)
     */
    public RiakResponse fetch(final String bucket, final String key, final FetchMeta fetchMeta) throws IOException {
        return clusterDelegate.execute(new ClusterTask<RiakResponse>() {
            public RiakResponse call(final RawClient client) throws IOException {
                return client.fetch(bucket, key, fetchMeta);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(final IRiakObject object, final StoreMeta storeMeta) throws IOException {
        return clusterDelegate.execute(new ClusterTask<RiakResponse>() {
            public RiakResponse call(final RawClient client) throws IOException {
                return client.store(object, storeMeta);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.IRiakObject
     * )
     */
    public void store(final IRiakObject object) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.store(object);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String)
     */
    public void delete(final String bucket, final String key) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.delete(bucket, key);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, int)
     */
    public void delete(final String bucket, final String key, final int deleteQuorum) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.delete(bucket, key, deleteQuorum);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String,
     * java.lang.String, com.basho.riak.client.raw.DeleteMeta)
     */
    public void delete(final String bucket, final String key, final DeleteMeta deleteMeta) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.delete(bucket, key, deleteMeta);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#listBuckets()
     */
    public Set<String> listBuckets() throws IOException {
        return clusterDelegate.execute(new ClusterTask<Set<String>>() {
            public Set<String> call(final RawClient client) throws IOException {
                return client.listBuckets();
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#fetchBucket(java.lang.String)
     */
    public BucketProperties fetchBucket(final String bucketName) throws IOException {
        return clusterDelegate.execute(new ClusterTask<BucketProperties>() {
            public BucketProperties call(final RawClient client) throws IOException {
                return client.fetchBucket(bucketName);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#updateBucket(java.lang.String,
     * com.basho.riak.client.bucket.BucketProperties)
     */
    public void updateBucket(final String name, final BucketProperties bucketProperties) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.updateBucket(name, bucketProperties);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#listKeys(java.lang.String)
     */
    public Iterable<String> listKeys(final String bucketName) throws IOException {
        return clusterDelegate.execute(new ClusterTask<Iterable<String>>() {
            public Iterable<String> call(final RawClient client) throws IOException {
                return client.listKeys(bucketName);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.client.raw
     * .query.LinkWalkSpec)
     */
    public WalkResult linkWalk(final LinkWalkSpec linkWalkSpec) throws IOException {
        return clusterDelegate.execute(new ClusterTask<WalkResult>() {
            public WalkResult call(final RawClient client) throws IOException {
                return client.linkWalk(linkWalkSpec);
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.client.raw
     * .query.MapReduceSpec)
     */
    public MapReduceResult mapReduce(final MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        try {
            return clusterDelegate.execute(new ClusterTask<MapReduceResult>() {
                public MapReduceResult call(final RawClient client) throws IOException, MapReduceTimeoutException {
                    return client.mapReduce(spec);
                }
            });
        } catch (ClusterTaskException cte) {
            cte.throwCauseIf(MapReduceTimeoutException.class);
            throw cte;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        return clusterDelegate.execute(new ClusterTask<byte[]>() {
            public byte[] call(final RawClient client) throws IOException {
                return client.generateAndSetClientId();
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#setClientId(byte[])
     */
    public void setClientId(final byte[] clientId) throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.setClientId(clientId);
                return null;
            }
        });
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        return clusterDelegate.execute(new ClusterTask<byte[]>() {
            public byte[] call(final RawClient client) throws IOException {
                return client.getClientId();
            }
        });
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#ping()
     */
    public void ping() throws IOException {
        clusterDelegate.execute(new ClusterTask<Void>() {
            public Void call(final RawClient client) throws IOException {
                client.ping();
                return null;
            }
        });
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.query.IndexQuery)
     */
    public List<String> fetchIndex(final IndexQuery indexQuery) throws IOException {
        return clusterDelegate.execute(new ClusterTask<List<String>>() {
            public List<String> call(final RawClient client) throws IOException {
                return client.fetchIndex(indexQuery);
            }
        });
    }

    public void shutdown() {
        clusterDelegate.shutdown();
    }

    public int getHealthyNodeCount() {
        return clusterDelegate.getHealthyClients().size();
    }

    public int getUnhealthyNodeCount() {
        return clusterDelegate.getUnhealthyClients().size();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#fetchIndex(com.basho.riak.client.raw.RawClient#stats()
     */
    public NodeStats stats() throws IOException {
        NodeStats nodeStats = null;
        for (RawClient rc : clusterDelegate.getHealthyClients()) {
            if (nodeStats == null) {
                nodeStats = rc.stats();
            } else {
                nodeStats.add(rc.stats());
            }
        }
        return nodeStats;
    }

    /*
     * (non-Javadoc)
     * @see com.basho.riak.client.raw.RawClient#getNodeName()
     */
    public String getNodeName() {
        boolean first = true;
        StringBuilder builder = new StringBuilder("[");
        for (RawClient client : clusterDelegate.getHealthyClients()) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            builder.append(client.getNodeName());
        }
        builder.append("]");
        return builder.toString();
    }

}
