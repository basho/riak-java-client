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
package com.basho.riak.client.raw.http;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.response.ListBucketsResponse;
import com.basho.riak.client.query.RiakStreamingRuntimeException;
import com.basho.riak.client.query.StreamingOperation;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Wraps the stream of keys from BucketResponse.getBucketInfo.getKeys in an
 * iterator, handles closing the underlying http stream when finished.
 * 
 * @author russell
 * @author Brian Roach <roach at basho dot com>
 */
public class BucketSource implements StreamingOperation<String>
{
    private static final Timer timer = new Timer("riak-client-key-stream-timeout-thread", true);
    private final ListBucketsResponse listBucketsResponse;
    private final Iterator<String> buckets;
    private ReaperTask reaper;
    
    /**
     * Create a Key Source from an http.{@link ListBucketsResponse} in response to
     * {@link RiakClient#listBuckets(boolean) request. The bucket response
     * contains a stream which you must close if you do not iterate over the entire set.
     * 
     * @param listBucketsResponse
     *            the {@link ListBucketsResponse} with a List of buckets.
     */
    public BucketSource(ListBucketsResponse listBucketsResponse) 
    {
        this.listBucketsResponse = listBucketsResponse;
        this.buckets = listBucketsResponse.getBuckets().iterator();
        this.reaper = new ReaperTask(this, listBucketsResponse);
    }

    public boolean hasNext() 
    {
        boolean hasNext = false;
        try 
        {
            hasNext = buckets.hasNext();
        }
        catch (RuntimeException re) 
        {
            throw new RiakStreamingRuntimeException(re);
        }
        finally 
        {
            if (!hasNext)
            {
                cancel();
            }
        }
        return hasNext;
    }

    public String next() 
    {
        if (!hasNext()) 
        {
            throw new NoSuchElementException();
        }
        return buckets.next();
    }

    public void cancel()
    {
        reaper.cancel();
        listBucketsResponse.close();
    }

    public Iterator<String> iterator()
    {
        return this;
    }

    public void remove()
    {
        throw new UnsupportedOperationException("Not supported");
    }

    public Set<String> getAll() 
    {
        Set<String> set = new HashSet<String>();
        while (hasNext()) {
            set.add(next());
        }
        return set;
    }

    public boolean hasContinuation()
    {
        return false;
    }

    public String getContinuation()
    {
        return null;
    }
    
    /**
     * The underlying stream is not exposed to the caller (it is an
     * implementation detail) This timer task ensures that the underlying HTTP
     * resource is closed when the iterator is no longer reachable.
     * 
     */
    static class ReaperTask extends TimerTask {
        private final ListBucketsResponse bucketResponse;
        private WeakReference<?> ref;

        ReaperTask(Object holder, ListBucketsResponse conn) {
            this.bucketResponse = conn;
            this.ref = new WeakReference<Object>(holder);
            BucketSource.timer.scheduleAtFixedRate(this, 500, 500);
        }

        @Override public synchronized void run() {
            if (ref == null) {
                // NO-OP
            } else if (ref.get() == null) {
                // the reference was lost; cancel this timer and
                // close the connection
                cancel();
                bucketResponse.close();
            }
        }

        @Override public synchronized boolean cancel() {
            ref = null;
            return super.cancel();
        }
    }
    
    
}
