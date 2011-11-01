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
 * Common, base, generic implementation for value match index queries.
 * 
 * @author russell
 * 
 */
public abstract class AbstractValueQuery<T> extends AbstractIndexQuery implements ValueQuery<T> {

    private final T value;

    /**
     * Called by concrete subclasses
     * 
     * @param indexName
     *            the full name of the index (including type suffix)
     * @param bucket
     *            the bucket to query
     */
    protected AbstractValueQuery(String indexName, String bucket, T value) {
        super(indexName, bucket);
        this.value = value;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.query.ValueQuery#getValue()
     */
    public T getValue() {
        return value;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        if (!(obj instanceof AbstractValueQuery)) {
            return false;
        }
        @SuppressWarnings("rawtypes") AbstractValueQuery other = (AbstractValueQuery) obj;
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }
}
