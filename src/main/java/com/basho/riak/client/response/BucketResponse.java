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
package com.basho.riak.client.response;

import com.basho.riak.client.RiakBucketInfo;

/**
 * Response from a GET request at a bucket's URL
 */
public interface BucketResponse extends HttpResponse {

    /**
     * @return Whether the bucket's schema and keys were returned in the
     *         response from Riak
     */
    public boolean hasBucketInfo();

    /**
     * @return The bucket's schema and keys
     */
    public RiakBucketInfo getBucketInfo();

}
