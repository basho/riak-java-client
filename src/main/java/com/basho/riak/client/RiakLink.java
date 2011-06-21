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
 * Models a link from one object to another in Riak.
 * <p>
 * Links are unidirectional and enable lightweight graph semantics in Riak.
 * See <a href="http://wiki.basho.com/Links.html">the basho wiki</a> for more details on links.
 * </p>
 * <p>
 * Immutable.
 * </p>
 * @author russell
 * 
 */
public class RiakLink {

    private final String bucket;
    private final String key;
    private final String tag;

    /**
     * Create a RiakLink from the specified parameters.
     * 
     * @param bucket
     *            the bucket
     * @param key
     *            the key
     * @param tag
     *            the link tag
     */
    public RiakLink(String bucket, String key, String tag) {
        this.bucket = bucket;
        this.key = key;
        this.tag = tag;
    }

    /**
     * Create a RiakLink that is a copy of another RiakLink.
     * 
     * @param riakLink
     *            the RiakLink to copy
     */
    public RiakLink(final RiakLink riakLink) {
        this.bucket = riakLink.getBucket();
        this.key = riakLink.getKey();
        this.tag = riakLink.getTag();
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

    /**
     * @return the tag
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
        if (!(obj instanceof RiakLink)) {
            return false;
        }
        RiakLink other = (RiakLink) obj;
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
