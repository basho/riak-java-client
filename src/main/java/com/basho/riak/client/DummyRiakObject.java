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

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @author russell
 *
 */
public class DummyRiakObject implements RiakObject {

    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<RiakLink> iterator() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getBucket()
     */
    public Bucket getBucket() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getVClock()
     */
    public VClock getVClock() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getKey()
     */
    public String getKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getVtag()
     */
    public String getVtag() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getLastModified()
     */
    public Date getLastModified() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getContentType()
     */
    public String getContentType() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getMeta()
     */
    public Map<String, String> getMeta() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getBucketName()
     */
    public String getBucketName() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.RiakObject#getValue()
     */
    public String getValue() {
        return null;
    }

}
