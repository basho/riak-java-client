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
package com.basho.riak.client.query.functions;

/**
 * A JS function that is stored in a Riak bucket/key location
 * @author russell
 *
 */
public class JSBucketKeyFunction implements AnonymousFunction {

    private final String bucket;
    private final String key;

    /**
     * @param bucket
     * @param key
     */
    public JSBucketKeyFunction(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }
    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }
    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }
    
    
}
