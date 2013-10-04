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
package com.basho.riak.client.operations;

import java.util.concurrent.Callable;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;

/**
 * An operation to delete some data item from Riak.
 * 
 * <p>
 * Use {@link Bucket#delete(String)} or {@link Bucket#delete(Object)} to create a delete operation.
 * Also look at {@link DomainBucket#delete(Object)}  and {@link DomainBucket#delete(String)}.
 * </p>
 * 
 * @author russell
 * @see Bucket
 * @see DomainBucket
 */
public class DeleteObject implements RiakOperation<Void> {

    private final RawClient client;
    private final String bucket;
    private final String key;

    private Retrier retrier;
    private boolean fetchBeforeDelete = false;

    private final DeleteMeta.Builder deleteMetaBuilder = new DeleteMeta.Builder();
    private final FetchMeta.Builder fetchMetaBuilder = new FetchMeta.Builder().returnDeletedVClock(true);

    /**
     * Create a <code>DeleteOperation</code> that delegates to
     * <code>client</code> to delete <code>key</code> from <code>bucket</code>
     * using <code>retrier</code> to handle failures. Use
     * {@link Bucket#delete(String)} or {@link Bucket#delete(Object)} to create
     * a delete operation.
     * 
     * @param client
     *            the {@link RawClient} to use
     * @param bucket
     *            the bucket for the key to delete
     * @param key
     *            the key of the item to delete
     * @param retrier
     *            the {@link Retrier} to use for the delete operation
     */
    public DeleteObject(RawClient client, String bucket, String key, final Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.retrier = retrier;
    }

    /**
     * Uses the specified {@link Retrier} to call the specified
     * {@link RawClient}'s delete with <code>bucket</code>, <code>key</code> and (if specified) the rest of the delete operation parameters.
     * If <code>fetchBeforeDelete</code> is true, then a fetch is performed first to get a vclock.
     * 
     * @return null, always null.
     */
    public Void execute() throws RiakException {
        if(fetchBeforeDelete) {
            Callable<RiakResponse> fetch = new Callable<RiakResponse>() {
                public RiakResponse call() throws Exception {
                    RiakResponse response = client.head(bucket, key, fetchMetaBuilder.build());
                    return response;
                }
            };
                
            RiakResponse response = retrier.attempt(fetch);
            // If there's only one object, and it's a tombstone ... just bail.
            // It's like, how much more deleted could this be? 
            // And the answer is none. None more deleted. 
            if (response.numberOfValues() == 1 && response.getRiakObjects()[0].isDeleted())
            {
                return null;
            }
            deleteMetaBuilder.vclock(response.getVclock());
        }

        Callable<Void> command = new Callable<Void>() {
            public Void call() throws Exception {
                client.delete(bucket, key, deleteMetaBuilder.build());
                return null;
            }
        };

        retrier.attempt(command);
        return null;
    }

    /**
     * @param r
     *            the read quorum for the delete operation
     * @return this
     */
    public DeleteObject r(int r) {
        fetchMetaBuilder.r(r);
        deleteMetaBuilder.r(r);
        return this;
    }

    /**
     * @param r
     *            the read quorum for the delete operation
     * @return this
     */
    public DeleteObject r(Quora r) {
        fetchMetaBuilder.r(r);
        deleteMetaBuilder.r(r);
        return this;
    }

    /**
     * @param r
     *            the read quorum for the delete operation
     * @return this
     */
    public DeleteObject r(Quorum r) {
        fetchMetaBuilder.r(r);
        deleteMetaBuilder.r(r);
        return this;
    }



    /**
     * @param pr
     *            the primary read quorum for the delete operation
     * @return this
     */
    public DeleteObject pr(int pr) {
        fetchMetaBuilder.pr(pr);
        deleteMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param pr
     *            the primary read quorum for the delete operation
     * @return this
     */
    public DeleteObject pr(Quora pr) {
        fetchMetaBuilder.pr(pr);
        deleteMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param pr
     *            the primary read quorum for the delete operation
     * @return this
     */
    public DeleteObject pr(Quorum pr) {
        fetchMetaBuilder.pr(pr);
        deleteMetaBuilder.pr(pr);
        return this;
    }
    
    /**
     * @param w
     *            the write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject w(int w) {
        deleteMetaBuilder.w(w);
        return this;
    }

    /**
     * @param w
     *            the write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject w(Quora w) {
        deleteMetaBuilder.w(w);
        return this;
    }

    /**
     * @param w
     *            the write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject w(Quorum w) {
        deleteMetaBuilder.w(w);
        return this;
    }

    /**
     * @param dw
     *            the durable write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject dw(int dw) {
         deleteMetaBuilder.dw(dw);
        return this;
    }

    /**
     * @param dw
     *            the durable write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject dw(Quora dw) {
         deleteMetaBuilder.dw(dw);
        return this;
    }


    /**
     * @param dw
     *            the durable write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject dw(Quorum dw) {
         deleteMetaBuilder.dw(dw);
        return this;
    }

    /**
     * @param pw
     *            the primary write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject pw(int pw) {
        deleteMetaBuilder.pw(pw);
        return this;
    }

    /**
     * @param pw
     *            the primary write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject pw(Quora pw) {
        deleteMetaBuilder.pw(pw);
        return this;
    }

    /**
     * @param pw
     *            the primary write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject pw(Quorum pw) {
        deleteMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * The read_write quorum for the delete operation
     * @param rw an {@link Integer} for the read/write quorum
     * @return this
     */
    public DeleteObject rw(int rw) {
        deleteMetaBuilder.rw(rw);
        return this;
    }

    /**
     * The read_write quorum for the delete operation
     * @param rw an {@link Integer} for the read/write quorum
     * @return this
     */
    public DeleteObject rw(Quora rw) {
        deleteMetaBuilder.rw(rw);
        return this;
    }

    /**
     * The read_write quorum for the delete operation
     * @param rw an {@link Integer} for the read/write quorum
     * @return this
     */
    public DeleteObject rw(Quorum rw) {
        deleteMetaBuilder.rw(rw);
        return this;
    }
    
    /**
     * Provide a vclock to riak for the delete operation.
     * 
     * <p>NOTE: you can, instead, <code>fetchBeforeDelete</code> to get a vclock</p>
     * @param vclock
     * @return this
     * @see DeleteObject#fetchBeforeDelete
     */
    public DeleteObject vclock(VClock vclock) {
        deleteMetaBuilder.vclock(vclock);
        return this;
    }

    /**
     * Set {@link Retrier} to use when executing this delete operation
     * @param retrier a {@link Retrier}
     * @return this
     */
    public DeleteObject withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }

    /**
     * If you want to provide a vclock to delete, but don't have one, setting
     * this true will have the operation first perform a fetch (using the
     * supplied r/pr parameters). The vclock from that fetch will then be used
     * in the delete operation
     * 
     * @param fetch
     *            true true to fetch before delete, false if not
     * @return this
     */
    public DeleteObject fetchBeforeDelete(boolean fetch) {
        this.fetchBeforeDelete = fetch;
        return this;
    }
    
    
    /**
     * Set an operation timeout in milliseconds to be sent to Riak
     * 
     * As of 1.4 Riak allows a timeout to be sent for get, put, and delete operations. 
     * The client will receive a timeout error if the operation is not completed 
     * within the specified time
     * 
     * @param timeout
     * @return this
     * 
     */
    
    public DeleteObject timeout(int timeout) {
        deleteMetaBuilder.timeout(timeout);
        return this;
    }
}
