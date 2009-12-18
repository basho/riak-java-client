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

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

import com.basho.riak.client.response.StoreResponse;

/**
 * A Riak object
 */
public interface RiakObject {

    /**
     * Copy the metadata and value from <code>object</code>. The bucket and key
     * are not copied.
     * 
     * @param object
     *            The source object to copy from
     */
    public void copyData(RiakObject object);

    /**
     * Update the object's metadata. This usually happens when Riak returns
     * updated metadata from a store operation.
     * 
     * @param response
     *            Response from a store operation containing an updated vclock,
     *            last modified date, and vtag
     */
    public void updateMeta(StoreResponse response);

    /**
     * @return The object's bucket
     */
    public String getBucket();

    /**
     * @return The object's key
     */
    public String getKey();

    /**
     * @return The object's value
     */
    public String getValue();

    /**
     * @return The object's links
     */
    public Collection<RiakLink> getLinks();

    /**
     * @return User-specified metadata for the object in the form of key-value
     *         pairs.
     */
    public Map<String, String> getUsermeta();

    /**
     * @return The object's content type as a MIME type
     */
    public String getContentType();

    /**
     * @return The object's opaque vclock assigned by Riak
     */
    public String getVclock();

    /**
     * @return The modification date of the object determined by Riak
     */
    public String getLastmod();

    /**
     * @return An entity tag for the object assigned by Riak
     */
    public String getVtag();

    /**
     * @return The actual entity to send to Riak for a store operation. When
     *         storing an object either getEntity() or getEntityStream() must be
     *         non-null.
     */
    public String getEntity();

    /**
     * @return The actual entity to send to Riak for a store operation provided
     *         as a stream. When storing an object either getEntity() or
     *         getEntityStream() must be non-null.
     */
    public InputStream getEntityStream();

    /**
     * @return The length of the stream provided by getEntityStream() or -1 if
     *         unknown. If unknown, the stream will be buffered in memory to
     *         determine the entity length.
     */
    public long getEntityStreamLength();
}