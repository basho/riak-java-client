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
package com.basho.riak.client.http;

/**
 * @author russell
 * 
 */
public class IntIndex extends RiakIndex<Integer> {

    public static final String SUFFIX = "_int";
    private final int value;

    /**
     * @param indexName
     */
    public IntIndex(String indexName, int value) {
        super(indexName);
        this.value = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.http.RiakIndex#getValue()
     */
    @Override public Integer getValue() {
        return value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + value;
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof IntIndex)) {
            return false;
        }
        IntIndex other = (IntIndex) obj;
        if (value != other.value) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("IntIndex [value=%s, getName()=%s]", value, getName());
    }
}
