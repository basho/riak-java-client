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
package com.basho.riak.newapi;

/**
 * Immutable RiakLink impl.
 * 
 * @author russell
 * 
 */
public class DefaultRiakLink implements RiakLink {

    private final String bucket;
    private final String key;
    private final String tag;

    /**
     * @param tag
     * @param bucket
     * @param key
     */
    public DefaultRiakLink(String bucket, String key, String tag) {
        this.tag = tag;
        this.bucket = bucket;
        this.key = key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.RiakLink#getBucket()
     */
    public String getBucket() {
        return bucket;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.RiakLink#getKey()
     */
    public String getKey() {
        return key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.RiakLink#getTag()
     */
    public String getTag() {
        return tag;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
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
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DefaultRiakLink)) {
            return false;
        }
        DefaultRiakLink other = (DefaultRiakLink) obj;
        if (bucket == null) {
            if (other.bucket != null) {
                return false;
            }
        } else if (!bucket.equals(other.bucket)) {
            return false;
        }
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        if (tag == null) {
            if (other.tag != null) {
                return false;
            }
        } else if (!tag.equals(other.tag)) {
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
        return String.format("DefaultRiakLink [tag=%s, bucket=%s, key=%s]", tag, bucket, key);
    }

}
