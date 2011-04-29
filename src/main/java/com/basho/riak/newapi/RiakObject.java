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

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import com.basho.riak.newapi.cap.VClock;

/**
 * @author russell
 * 
 */
public interface RiakObject extends Iterable<RiakLink> {

    String getBucket();

    String getValue();

    VClock getVClock();

    String getKey();

    String getVtag();

    Date getLastModified();

    String getContentType();

    // links
    Collection<RiakLink> getLinks();

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

    RiakObject setValue(String value);

    RiakObject setContentType(String contentType);

    /**
     * Add link to this RiakObject's links.
     * 
     * @param link
     *            a {@link RiakLink} to add.
     * @return this RiakObject.
     */
    RiakObject addLink(RiakLink link);

    /**
     * Remove a {@link RiakLink} from this RiakObject.
     * 
     * @param link
     *            the {@link RiakLink} to remove
     * @return this RiakObject
     */
    RiakObject removeLink(final RiakLink link);

    /**
     * Adds the key, value to the collection of user meta for this object.
     * 
     * @param key
     * @param value
     * @return this RiakObject.
     */
    RiakObject addUsermeta(String key, String value);

    /**
     * @param key
     *            the key of the item to remove
     */
    RiakObject removeUsermeta(String key);

    /**
     * @return A String of the VClock
     */
    String getVClockAsString();

}
