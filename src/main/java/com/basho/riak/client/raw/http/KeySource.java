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
package com.basho.riak.client.raw.http;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.query.RiakStreamingRuntimeException;
import com.basho.riak.client.query.StreamingOperation;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Wraps the stream of keys from BucketResponse.getBucketInfo.getKeys in an
 * iterator, handles closing the underlying http stream when finished.
 * 
 * @author russell
 */
public class KeySource implements StreamingOperation<String> {

    private static final Timer timer = new Timer("riak-client-key-stream-timeout-thread", true);
    private final BucketResponse bucketResponse;
    private final Iterator<String> keys;
    private ReaperTask reaper;

    /**
     * Create a Key Source from an http.{@link BucketResponse} in response to
     * {@link RiakClient#streamBucket(String)} request. The bucket response
     * contains a stream which you must close if you do not iterate over the entire set.
     * 
     * @param bucketResponse
     *            the {@link BucketResponse} with a List of keys.
     */
    public KeySource(BucketResponse bucketResponse) {
        this.bucketResponse = bucketResponse;
        this.keys = bucketResponse.getBucketInfo().getKeys().iterator();
        this.reaper = new ReaperTask(this, bucketResponse);
    }

    public boolean hasNext() {

        boolean hasNext = false;
        
        try {
            hasNext = keys.hasNext();
        } catch (RuntimeException re) {
            throw new RiakStreamingRuntimeException(re);
        } finally {
            // If there are no more keys, close the underlying HTTP resource
            // and cancel the timer
            if (!hasNext) {
                cancel();
            }
        }
        return hasNext;
    }

    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return keys.next();
    }

    public void cancel() {
        reaper.cancel();
        bucketResponse.close();
    }
    
    /**
     * This is a read only stream of keys, calling this results in
     * UnsupportedOperationException
     * 
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Set<String> getAll() 
    {
        Set<String> set = new HashSet<String>();
        while (hasNext()) {
            set.add(next());
        }
        return set;
    }

    public Iterator<String> iterator()
    {
        return this;
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
        private final BucketResponse bucketResponse;
        private WeakReference<?> ref;

        ReaperTask(Object holder, BucketResponse conn) {
            this.bucketResponse = conn;
            this.ref = new WeakReference<Object>(holder);
            KeySource.timer.scheduleAtFixedRate(this, 500, 500);
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
