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
/**
 * A bucket is a namespace abstraction provided by Riak, the API uses
 * {@link com.basho.riak.client.bucket.Bucket} as the primary way to interact
 * with data stored in Riak.
 * <p>
 * All data in Riak is stored under a bucket/key namespace. After you have
 * obtained a {@link com.basho.riak.client.bucket.Bucket} from the
 * {@link com.basho.riak.client.IRiakClient}, use it to fetch, store and delete
 * data.
 * </p>
 * <p>
 * For example
 * 
 * <code>
 * <pre>
 * 
 *   final String bucketName = UUID.randomUUID().toString();
 *   
 *   Bucket b = client.fetchBucket(bucketName).execute();
 *   //store something
 *   IRiakObject o = b.store("k", "v").execute();
 *   //fetch it back
 *   IRiakObject fetched = b.fetch("k").execute();
 *   // now update that riak object
 *   b.store("k", "my new value").execute();
 *   //fetch it back again
 *   fetched = b.fetch("k").execute();
 *   //delete it
 *   b.delete("k").execute();
 * </pre></code>
 * </p>
 * <p>
 * {@link com.basho.riak.client.bucket.Bucket} extends the
 * {@link com.basho.riak.client.bucket.BucketProperties} interface for access to
 * bucket schema information (like <code>n_val</code>, <code>allow_mult</code>,
 * default <code>r quorum</code> etc.)
 * </p>
 * <p>
 * This package also provides a {@link com.basho.riak.client.bucket.DomainBucket} for
 * wrapping a {@link com.basho.riak.client.bucket.Bucket}. A
 * {@link com.basho.riak.client.bucket.DomainBucket} simplifies working with a
 * bucket that only has one type of data in it.
 * {@link com.basho.riak.client.bucket.RiakBucket} is a
 * {@link com.basho.riak.client.bucket.DomainBucket} for working with
 * {@link com.basho.riak.client.IRiakObject}s.
 * 
 * @see com.basho.riak.client.bucket.Bucket
 * @see com.basho.riak.client.bucket.DomainBucket
 * @see com.basho.riak.client.bucket.RiakBucket
 */
package com.basho.riak.client.bucket;