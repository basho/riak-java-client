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
package com.basho.riak.client.query.indexes;

/**
 * Special BinIndex with no suffix, name $bucket, that provides range access to
 * keys in a bucket.
 * 
 * @author Randy Secrist
 */
public class BucketIndex extends BinIndex {

    private static final String BUCKETS_INDEX = "$bucket";
    private static final String EMPTY = "";

    public static final BucketIndex index = new BucketIndex();

    private BucketIndex() {
        super(BUCKETS_INDEX);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.query.indexes.BinIndex#getSuffix()
     */
    @Override protected String getSuffix() {
        return EMPTY;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.query.indexes.RiakIndex#getName()
     */
    @Override public String getName() {
        return BUCKETS_INDEX;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.query.indexes.RiakIndex#getFullname()
     */
    @Override public String getFullname() {
        return getName();
    }

}
