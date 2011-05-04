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
package com.basho.riak.client;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.FetchBucket;
import com.basho.riak.client.bucket.WriteBucket;
import com.basho.riak.client.query.BucketKeyMapReduce;
import com.basho.riak.client.query.BucketMapReduce;
import com.basho.riak.client.query.LinkWalk;

/**
 * @author russell
 * 
 */
public interface IRiakClient {

    IRiakClient setClientId(byte[] clientId) throws RiakException;

    byte[] generateAndSetClientId() throws RiakException;

    byte[] getClientId() throws RiakException;

    FetchBucket fetchBucket(String bucketName);

    WriteBucket updateBucket(Bucket b);

    WriteBucket createBucket(String string);

    // query - links
    LinkWalk walk(final IRiakObject startObject);

    // query - m/r
    
    /**
     * Map reduce over a set of bucket, key inputs
     */
    BucketKeyMapReduce mapReduce();
    
    /**
     * Map reduce over a bucket
     * @param bucket
     * @return
     */
    BucketMapReduce mapReduce(String bucket);
}
