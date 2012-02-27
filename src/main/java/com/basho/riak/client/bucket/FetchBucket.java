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
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.raw.RawClient;

/**
 * A {@link RiakOperation} that gets a {@link Bucket} from Riak.
 * 
 * <p>
 * Calls the underlying {@link RiakClient}s fetch method via the {@link Retrier}
 * attempt method, builds a {@link Bucket} from the response.
 * </p>
 * <p>
 * Example:
 * <code><pre>
 *   final String bucketName = "userAccounts";
 *   // fetch a bucket
 *   Bucket b = client.fetchBucket(bucketName).execute();
 *   // use the bucket
 *   IRiakObject o = b.store("k", "v").execute();
 * </pre></code>
 * </p>
 * @author russell
 */
public class FetchBucket implements RiakOperation<Bucket> {

    private final RawClient client;
    private final String bucket;

    private Retrier retrier;
    private boolean lazyLoadProperties = false;

    /**
     * Create a FetchBucket that delegates to the provided {@link RawClient}.
     * 
     * @param client the {@link RawClient} to use when fetching the bucket data.
     * @param bucket the name of the bucket to fetch.
     * @param retrier the {@link Retrier} to use when fetching bucket data.
     */
    public FetchBucket(final RawClient client, String bucket, final Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.retrier = retrier;
    }

    /**
     * Execute the fetch operation using the RawClient
     * @return a {@link Bucket} configured to use this instances {@link RawClient} and {@link Retrier} for its operations
     * @throws RiakRetryFailedException if the {@link Retrier} throws {@link RiakRetryFailedException}
     */
    public Bucket execute() throws RiakRetryFailedException {
        BucketProperties properties;
        if (!lazyLoadProperties) {
            properties = retrier.attempt(new Callable<BucketProperties>() {
                public BucketProperties call() throws Exception {
                    return client.fetchBucket(bucket);
                }
            });
        }
        else
        {
            properties = new LazyBucketProperties(client, retrier, bucket);
        }
        
        return new DefaultBucket(bucket, properties, client, retrier);
    }

    /**
     * Provide a {@link Retrier} to use for the fetch operation.
     *
     * @param retrier the {@link Retrier} to use
     * @return this
     */
    public FetchBucket withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }
    
    /**
     * Prior to the addition of this method there was no way to prevent 
     * {@link #execute() } from fetching the {@link BucketProperties} from Riak. 
     * <p>
     * Calling this prior to {@link #execute() } allows you to defer fetching 
     * the bucket properties for this bucket from Riak
     * until they are required by one of the {@link Bucket} methods that
     * accesses them (e.g. {@link Bucket#getR() } ). If none of those methods are
     * called then they are never retrieved.
     * </p>
     * @return this 
     * @since 1.0.4
     */
    public FetchBucket lazyLoadBucketProperties() {
        this.lazyLoadProperties = true;
        return this;
    }
    
 }
