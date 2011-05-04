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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.basho.riak.client.RiakLink;
import com.basho.riak.newapi.cap.VClock;

/**
 * Represents the data and meta data stored in Riak for bucket/key.
 * 
 * NOTE: The name will be changing soon. The initial Java client release
 * laid claim to the best name real estate. 
 * This class will be named RiakObject in subsequent releases.
 * 
 * @see DefaultRiakObject in the legacy project
 * @author russell
 * 
 */
public interface IRiakObject extends Iterable<RiakLink> {

    String getBucket();

    String getValue();

    VClock getVClock();

    String getKey();

    String getVtag();

    Date getLastModified();

    String getContentType();

    // links
    List<RiakLink> getLinks();

    boolean hasLinks();

    int numLinks();

    boolean hasLink(final RiakLink riakLink);

    // user meta
    Map<String, String> getMeta();

    boolean hasUsermeta();

    boolean hasUsermeta(String key);

    String getUsermeta(String key);

    Iterable<Entry<String, String>> userMetaEntries();

    // Mutate

    void setValue(String value);

    void setContentType(String contentType);

    /**
     * Add link to this RiakObject's links.
     * 
     * @param link
     *            a {@link RiakLink} to add.
     * @return this RiakObject.
     */
    IRiakObject addLink(RiakLink link);

    /**
     * Remove a {@link RiakLink} from this RiakObject.
     * 
     * @param link
     *            the {@link RiakLink} to remove
     * @return this RiakObject
     */
    IRiakObject removeLink(final RiakLink link);

    /**
     * Adds the key, value to the collection of user meta for this object.
     * 
     * @param key
     * @param value
     * @return this RiakObject.
     */
    IRiakObject addUsermeta(String key, String value);

    /**
     * @param key
     *            the key of the item to remove
     */
    IRiakObject removeUsermeta(String key);

    /**
     * @return A String of the VClock
     */
    String getVClockAsString();

}
