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

import com.basho.riak.client.http.response.BucketResponse;

/**
 * Wraps the stream of keys from BucketResponse.getBucketInfo.getKeys in an
 * iterator that handles closing the underlying http stream when finished with.
 * 
 * @author russell
 * 
 */
public class KeySource implements Iterator<String> {

    private static final Timer timer = new Timer();
    private final BucketResponse bucketResponse;
    private final Iterator<String> keys;
    private ReaperTask reaper;

    /**
     * @param bucketResponse
     */
    public KeySource(BucketResponse bucketResponse) {
        this.bucketResponse = bucketResponse;
        this.keys = bucketResponse.getBucketInfo().getKeys().iterator();
        this.reaper = new ReaperTask(this, bucketResponse);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {

        boolean hasNext = keys.hasNext();
        // If there are no more keys, close the underlying HTTP resource
        // and cancel the timer
        if (!hasNext) {
            reaper.cancel();
            bucketResponse.close();
        }

        return hasNext;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    public String next() {
        return keys.next();
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

    /**
     * The underlying stream is not exposed to the caller (it is an
     * implementation detail) This time task ensures that the underlying HTTP
     * resource is closed when the iterator is no longer reachable.
     * 
     * @author russell
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
