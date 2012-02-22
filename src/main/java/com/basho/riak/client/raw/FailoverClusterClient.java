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

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.raw.query.indexes.IndexQuery;

import java.io.IOException;
import java.util.*;

public abstract class FailoverClusterClient<T extends Configuration> implements RawClient {
  //protected static Log log=LogFactory.getLog("com.temetra.riak.fcc");
  private FailoverNodes nodes;

  public FailoverClusterClient(ClusterConfig<T> clusterConfig) throws IOException
  {
    //noinspection AbstractMethodCallInConstructor
    nodes=new FailoverNodes(fromConfig(clusterConfig));
  }

  /**
   * Create an array of clients for the cluster from the given
   * {@link com.basho.riak.client.raw.config.ClusterConfig}.
   *
   * @param clusterConfig starting point
   * @return the array of {@link RawClient} delegates that make up the cluster
   * @throws java.io.IOException when there's a problem
   */
  protected abstract RawClient[] fromConfig(ClusterConfig<T> clusterConfig) throws IOException;

  private RawClient getDelegate() {
    return null;
  }

  public RiakResponse head(final String bucket, final String key, final FetchMeta fetchMeta) throws IOException
  {
    return (RiakResponse)attempt(new CallableForNode<RiakResponse>() {
        public RiakResponse call(RawClient riak) throws IOException
        {
          return riak.head(bucket,key,fetchMeta);
        }
    });
  }

  public RiakResponse fetch(final String bucket, final String key) throws IOException
  {
    return (RiakResponse)attempt(new CallableForNode<RiakResponse>() {
        public RiakResponse call(RawClient riak) throws IOException
        {
          return riak.fetch(bucket,key);
        }
    });
  }

  public RiakResponse fetch(final String bucket, final String key, final int readQuorum) throws IOException
  {
    return (RiakResponse)attempt(new CallableForNode<RiakResponse>() {
        public RiakResponse call(RawClient riak) throws IOException
        {
          return riak.fetch(bucket,key,readQuorum);
        }
    });
  }

  public RiakResponse fetch(final String bucket, final String key, final FetchMeta fetchMeta) throws IOException
  {
    return (RiakResponse)attempt(new CallableForNode<RiakResponse>() {
        public RiakResponse call(RawClient riak) throws IOException
        {
          return riak.fetch(bucket,key,fetchMeta);
        }
    });
  }

  public RiakResponse store(final IRiakObject object, final StoreMeta storeMeta) throws IOException
  {
    return (RiakResponse)attempt(new CallableForNode<RiakResponse>() {
        public RiakResponse call(RawClient riak) throws IOException
        {
          return riak.store(object,storeMeta);
        }
    });
  }

  public void store(final IRiakObject object) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.store(object);
          return null;
        }
    });
  }

  public void delete(final String bucket, final String key) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.delete(bucket,key);
          return null;
        }
    });
  }

  public void delete(final String bucket, final String key, final int deleteQuorum) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.delete(bucket,key,deleteQuorum);
          return null;
        }
    });
  }

  public void delete(final String bucket, final String key, final DeleteMeta deleteMeta) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.delete(bucket,key,deleteMeta);
          return null;
        }
    });
  }

  public Set<String> listBuckets() throws IOException
  {
    //noinspection unchecked
    return (Set<String>) attempt(new CallableForNode<Set<String>>() {
        public Set<String> call(RawClient riak) throws IOException
        {
            return riak.listBuckets();
        }
    });
  }

  @SuppressWarnings({"SerializableInnerClassWithNonSerializableOuterClass","SerializableNonStaticInnerClassWithoutSerialVersionUID"})
  class FailoverNodes extends ArrayList<FailoverNode>{
    private int nextCheckIndex=0;

    public FailoverNodes(RawClient[] rawClients)
    {
      for(RawClient next : rawClients){
        add(new FailoverNode(next));
      }
    }

    private ActiveNodesIterator activeNodes()
    {
      return new ActiveNodesIterator(this,nextCheckIndex());
    }

    /**
     * Simple load balancing - each thread will use an iterator for active nodes with a
     * different starting point, so round-robin
     * @return the next valid index will be starting point for this Iterator
     */
    private synchronized int nextCheckIndex()
    {
      nextCheckIndex=(nextCheckIndex+1) % nodes.size();
      return nextCheckIndex;
    }
  }

  class FailoverNode
  {
    private RawClient riak=null;
    private long nextActiveCheckMillis=0;

    FailoverNode(RawClient in)
    {
      riak=in;
    }

    public RawClient getRiak()
    {
      return riak;
    }

    public String toString()
    {
      return riak!=null?riak.getTransport().name():"null";
    }

    public void markAsSuspect()
    {
      nextActiveCheckMillis=System.currentTimeMillis()+15000;
      //log.debug("*** Marking "+riak+" as suspect until "+ Conversion.formatIrishTime(nextActiveCheckMillis));
    }

    public boolean isActive() // Don't bother synchronizing - fails safe
    {
      if(nextActiveCheckMillis==0){
        return true;
      }
      if(System.currentTimeMillis()>nextActiveCheckMillis){
        nextActiveCheckMillis=0;
      }
      return nextActiveCheckMillis==0;
    }

    public void shutdown()
    {
      riak.shutdown();
    }
  }

  /**
   * Iterates over nodes, picking only active ones
   * Takes a start index so caller can prefer particular nodes (eg load balancing)
   */
  class ActiveNodesIterator implements Iterator<FailoverNode>
  {
    private List<FailoverNode> list;
    private int iterationsCount=0;
    private int currentIndex; // -1 if we we've traversed all active nodes

    public ActiveNodesIterator(List<FailoverNode> list,int startIndex)
    {
      this.list=list;
      currentIndex=nextActiveNodeFrom(startIndex);
    }

    private int nextActiveNodeFrom(int i)
    {
      final int size=nodes.size();
      while(true){
        i=i % size;
        if(list.get(i).isActive()){
          return i;
        }
        i++;
        if(++iterationsCount>size){
          return -1;
        }
      }
    }

    public boolean hasNext()
    {
      return (currentIndex>=0);
    }

    public FailoverNode next()
    {
      if(currentIndex<0){
        throw new NoSuchElementException();
      }
      FailoverNode r=list.get(currentIndex);
      currentIndex=nextActiveNodeFrom(currentIndex+1);
      return r;
    }

    public void remove()
    {
      // NOOP
    }
  }

  private Object attempt(CallableForNode callableForNode) throws IOException
  {
    long timeoutMillis=System.currentTimeMillis()+2000L;
    // Once around the active nodes without delays
    ActiveNodesIterator iter=nodes.activeNodes();
    while(iter.hasNext()){
      final FailoverNode next=iter.next();
      try{
        //log.debug("  Calling with "+next.getRiak());
        return callableForNode.call(next.getRiak());
      } catch(Exception e){
        //log.warn("Exception when calling with node "+next+"/"+e.getMessage());
        next.markAsSuspect();
      }
    }
    //log.debug("  All failed - trying everything");
    // Didn't succeed with any of the likely candidates, so keep trying all nodes until we time out
    while(System.currentTimeMillis()<timeoutMillis){
      for(FailoverNode next : nodes){
        try{
          return callableForNode.call(next.getRiak());
        } catch(Exception e){
          //log.warn("Exception when calling with node "+next+"/"+e.getMessage());
        }
      }
      try{
        Thread.sleep(50);
      } catch(InterruptedException e){
        break;
      }
    }
    throw new IOException("No failover node succeeded");
  }

  public BucketProperties fetchBucket(final String bucketName) throws IOException
  {
    return (BucketProperties)attempt(new CallableForNode<BucketProperties>() {
        public BucketProperties call(RawClient riak) throws IOException
        {
          return riak.fetchBucket(bucketName);
        }
    });
  }

  public void updateBucket(final String name, final BucketProperties
          bucketProperties) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.updateBucket(name,bucketProperties);
          return null;
        }
    });
  }

  public Iterable<String> listKeys(final String bucketName) throws IOException
  {
    //noinspection unchecked
    return (Iterable<String>)attempt(new CallableForNode<Iterable<String>>() {
        public Iterable<String> call(RawClient riak) throws IOException
        {
          return riak.listKeys(bucketName);
        }
    });
  }

  public WalkResult linkWalk(final LinkWalkSpec
          linkWalkSpec) throws IOException
  {
    return (WalkResult)attempt(new CallableForNode<WalkResult>() {
        public WalkResult call(RawClient riak) throws IOException
        {
          return riak.linkWalk(linkWalkSpec);
        }
    });
  }

  public MapReduceResult mapReduce(final MapReduceSpec
          spec) throws IOException, MapReduceTimeoutException
  {
    return (MapReduceResult)attempt(new CallableForNode<MapReduceResult>() {
        public MapReduceResult call(RawClient riak) throws IOException, MapReduceTimeoutException
        {
          return riak.mapReduce(spec);
        }
    });
  }

  public byte[] generateAndSetClientId() throws IOException
  {
    return (byte[])attempt(new CallableForNode<byte[]>() {
        public byte[] call(RawClient riak) throws IOException
        {
          return riak.generateAndSetClientId();
        }
    });
  }

  public void setClientId(final byte[] clientId) throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.setClientId(clientId);
          return null;
        }
    });
  }

  public byte[] getClientId() throws IOException
  {
    return (byte[])attempt(new CallableForNode<byte[]>() {
        public byte[] call(RawClient riak) throws IOException
        {
          return riak.getClientId();
        }
    });
  }

  public void ping() throws IOException
  {
    attempt(new CallableForNode<Void>() {
        public Void call(RawClient riak) throws IOException
        {
          riak.ping();
          return null;
        }
    });
  }

  public List<String> fetchIndex(IndexQuery
          indexQuery) throws IOException
  {
    final RawClient delegate = getDelegate();
    return delegate.fetchIndex(indexQuery);
  }

  public void shutdown(){
    for(FailoverNode rc : nodes){
      rc.shutdown();
    }
  }
}
