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
 * Base implementation of common behaviour for an {@link IndexQuery}
 * 
 * @author russell
 * 
 */
public abstract class AbstractIndexQuery implements IndexQuery {

    private final String index;
    private final String bucket;

    /**
     * @param indexName
     * @param bucket
     */
    protected AbstractIndexQuery(String indexName, String bucket) {
        this.index = indexName;
        this.bucket = bucket;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.query.IndexQuery#getIndex()
     */
    public String getIndex() {
        return this.index;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.query.IndexQuery#getBucket()
     */
    public String getBucket() {
        return this.bucket;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
        result = prime * result + ((index == null) ? 0 : index.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AbstractIndexQuery)) {
            return false;
        }
        AbstractIndexQuery other = (AbstractIndexQuery) obj;
        if (bucket == null) {
            if (other.bucket != null) {
                return false;
            }
        } else if (!bucket.equals(other.bucket)) {
            return false;
        }
        if (index == null) {
            if (other.index != null) {
                return false;
            }
        } else if (!index.equals(other.index)) {
            return false;
        }
        return true;
    }
}
