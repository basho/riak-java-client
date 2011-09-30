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

    private Integer r;
    private Integer pr;
    private Integer w;
    private Integer dw;
    private Integer pw;
    private Integer rw;
    private VClock vclock;

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
            Callable<VClock> fetch = new Callable<VClock>() {
                public VClock call() throws Exception {
                    // TODO this should be a head only operation for efficiency,
                    // change when implemented
                    RiakResponse response = client.fetch(bucket, key, new FetchMeta(r, pr, null, null, null, null,
                                                                                    null, null));
                    return response.getVclock();
                }
            };

            this.vclock = retrier.attempt(fetch);
        }

        Callable<Void> command = new Callable<Void>() {
            public Void call() throws Exception {
                client.delete(bucket, key, new DeleteMeta(r, pr, w, dw, pw, rw, vclock));
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
    public DeleteObject r(Integer r) {
        this.r = r;
        return this;
    }

    /**
     * @param pr
     *            the primary read quorum for the delete operation
     * @return this
     */
    public DeleteObject pr(Integer pr) {
        this.pr = pr;
        return this;
    }

    /**
     * @param w
     *            the write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject w(Integer w) {
        this.w = w;
        return this;
    }

    /**
     * @param dw
     *            the durable write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject dw(Integer dw) {
        this.dw = dw;
        return this;
    }

    /**
     * @param pw
     *            the primary write quorum for the delete tombstone
     * @return this
     */
    public DeleteObject pw(Integer pw) {
        this.pw = pw;
        return this;
    }

    /**
     * The read_write quorum for the delete operation
     * @param rw an {@link Integer} for the read/write quorum
     * @return this
     */
    public DeleteObject rw(Integer rw) {
        this.rw = rw;
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
        this.vclock = vclock;
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
}
