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

/**
 * Base, common, generic {@link RangeQuery} implementation
 * 
 * @author russell
 * @param <T>
 *            the Type for the query index parameters
 * 
 */
public abstract class AbstractRangeQuery<T> extends AbstractIndexQuery implements RangeQuery<T> {

    private final T from;
    private final T to;

    /**
     * Called by concrete implementations
     * 
     * @param indexName
     *            the full name (including suffix) of the index
     * @param bucket
     *            the bucket to query
     */
    protected AbstractRangeQuery(String indexName, String bucket, T from, T to) {
        super(indexName, bucket);
        this.from = from;
        this.to = to;
    }

    /**
     * The start value for the range query
     * 
     * @return the from
     */
    public T from() {
        return from;
    }

    /**
     * The end value for the range query
     * 
     * @return the to
     */
    public T to() {
        return to;
    }
}
