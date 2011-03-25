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

/**
 * @author russell
 *
 */
public class FetchBucket implements RiakOperation<Bucket> {

    private int retry = 0;
    private boolean fetchKeys = false;
    private boolean fetchProperties = true;
    
    public Bucket execute() {
        return null;
    }

    public FetchBucket retry(int i) {
        this.retry = i;
        return this;
    }

    public FetchBucket fetchKeys(boolean fetchKeys) {
        this.fetchKeys  = fetchKeys;
        return this;
    }

    public FetchBucket fetchProperties(boolean fetchProperties) {
        this.fetchProperties  = fetchProperties;
        return this;
    }

}
