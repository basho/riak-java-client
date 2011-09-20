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
package com.basho.riak.client.raw.query.indexes;

import java.io.IOException;

import com.basho.riak.client.query.indexes.BinIndex;

/**
 * Concrete value query for a {@link BinIndex}
 * 
 * @author russell
 * 
 */
public class BinValueQuery extends AbstractValueQuery<String> {

    /**
     * Create a query that matches the given value for the given index
     * 
     * @param index
     *            the index to query
     * @param bucket
     *            the bucket to query
     * @param value
     *            the value to match
     */
    public BinValueQuery(BinIndex index, String bucket, String value) {
        super(index.getFullname(), bucket, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.query.indexes.IndexQuery#write(com.basho.riak
     * .client.raw.query.indexes.IndexWriter)
     */
    public void write(IndexWriter executor) throws IOException {
        executor.write(getBucket(), getIndex(), getValue());
    }

}
