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
package com.basho.riak.client.bucket;

import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class FetchBucket implements RiakOperation<Bucket> {

    private final RawClient client;
    private final String bucket;

    private Retrier retrier;

    /**
     * @param client
     * @param bucket
     */
    public FetchBucket(RawClient client, String bucket, final Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.retrier = retrier;
    }

    /**
     * Execute the fetch operation using the RawClient
     */
    public Bucket execute() throws RiakRetryFailedException {
        BucketProperties properties = retrier.attempt(new Callable<BucketProperties>() {
            public BucketProperties call() throws Exception {
                return client.fetchBucket(bucket);
            }
        });

        return new DefaultBucket(bucket, properties, client, retrier);
    }

    /**
     * Provide a retrier to use for the fetch operation.
     *
     * @param retrier the Retrier to use
     * @return this
     */
    public FetchBucket retrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
 }
