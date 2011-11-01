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

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((from == null) ? 0 : from.hashCode());
        result = prime * result + ((to == null) ? 0 : to.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof AbstractRangeQuery)) {
            return false;
        }
        @SuppressWarnings("rawtypes") AbstractRangeQuery other = (AbstractRangeQuery) obj;
        if (from == null) {
            if (other.from != null) {
                return false;
            }
        } else if (!from.equals(other.from)) {
            return false;
        }
        if (to == null) {
            if (other.to != null) {
                return false;
            }
        } else if (!to.equals(other.to)) {
            return false;
        }
        return true;
    }
}
