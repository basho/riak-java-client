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

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.raw.RawClient;

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

    private Integer rw;

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
     * {@link RawClient}'s delete with <code>bucket</code>, <code>key</code> and (if specified) <code>rw</code>
     * @return null, always null.
     */
    public Void execute() throws RiakRetryFailedException {
        Callable<Void> command = new Callable<Void>() {
            public Void call() throws Exception {
                if (rw == null) {
                    client.delete(bucket, key);
                } else {
                    client.delete(bucket, key, rw);
                }
                return null;
            }
        };

        retrier.attempt(command);
        return null;
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
     * Set {@link Retrier} to use when executing this delete operation
     * @param retrier a {@link Retrier}
     * @return this
     */
    public DeleteObject retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
}
