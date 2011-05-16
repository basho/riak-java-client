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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.LinkWalk;

/**
 * Represents the data and meta data stored in Riak at a bucket/key.
 * 
 * <p>
 * Although you can store your own Java Beans in Riak, this interface represents
 * the core data type that is passed between the low and high level APIs and
 * that all POJOs are converted to and from when stored or fetched.
 * </p>
 * 
 * <p>
 * Extends {@link Iterable} to provide a simple way to iterate over the
 * collection of {@link RiakLink}s.
 * </p>
 * 
 * <p>
 * NOTE: The name will be changing soon. The initial Java client release laid
 * claim to the best name real estate. This class will be named RiakObject in
 * subsequent releases.
 * </p>
 * 
 * @author russell
 * 
 */
public interface IRiakObject extends Iterable<RiakLink> {

    /**
     * The name of this objects bucket
     * 
     * @return the bucket name.
     */
    String getBucket();

    /**
     * The value.
     * 
     * @return byte[] of this object value.
     */
    byte[] getValue();

    /**
     * Convenience method. Will use the content-type to figure out the charset.
     * 
     * @return the byte[] coerced to a String using the object's content-type
     */
    String getValueAsString();

    /**
     * This objects Vector Clock.
     * 
     * See the <a href="http://wiki.basho.com/Vector-Clocks.html">basho wiki</a>
     * for more on vector clocks
     * 
     * @return the {@link VClock} for this object.
     */
    VClock getVClock();

    /**
     * String copy of this object's vector clock.
     * 
     * @return A String of this objects Vector Clock
     */
    String getVClockAsString();

    /**
     * The object's key.
     * 
     * @return The objects key.
     */
    String getKey();

    /**
     * If this object has a version tag (if it is one of a set of siblings)
     * 
     * @return the vtag, if present.
     */
    String getVtag();

    /**
     * The last modified date as held by Riak.
     * 
     * @return the last modified date as returned from Riak.
     */
    Date getLastModified();

    /**
     * The content-type of this object's value.
     * 
     * @return the Objects Content-Type. If you don't set this it defaults to
     *         {@link DefaultRiakObject#DEFAULT_CONTENT_TYPE}
     */
    String getContentType();

    /**
     * A List of {@link RiakLink}s from this object. See also <a
     * href="http://wiki.basho.com/Links.html">Link Walking</a> on the basho
     * site.
     * 
     * @return The List of RiakLinks from this object
     * @see RiakLink
     * @see LinkWalk
     */
    List<RiakLink> getLinks();

    /**
     * Does this object link to any others?
     * 
     * @return true if this object has any links, false otherwise.
     */
    boolean hasLinks();

    /**
     * How many {@link RiakLink}s does this object have.
     * 
     * @return the number of links from this object.
     */
    int numLinks();

    /**
     * Does this object have that link?
     * 
     * @param riakLink
     *            a {@link RiakLink}
     * @return true if this object's link collection contains the passed
     *         {@link RiakLink}, false otherwise.
     */
    boolean hasLink(final RiakLink riakLink);

    /**
     * User meta data can be added to any Riak object. They are a String
     * key/value pairs that are stored with the IRiakObject riak.
     * 
     * See <a
     * href="http://wiki.basho.com/REST-API.html#Object-Key-operations">basho
     * wiki</a> for more details.
     * 
     * @return the {@link Map} of meta data for this object.
     */
    Map<String, String> getMeta();

    /**
     * Does this object have any user meta data?
     * 
     * @return if this IRiakObject has any user meta data items.
     */
    boolean hasUsermeta();

    /**
     * Does this object have a meta data item for that key?
     * 
     * @param key
     * @return true if this IRiakObject's user meta data contains the supplied
     *         key
     */
    boolean hasUsermeta(String key);

    /**
     * Get the user meta data item for that key.
     * 
     * @param key
     *            the name of the user meta data item
     * @return a String of the user meta data item or null if no item present
     *         for the supplied key
     */
    String getUsermeta(String key);

    /**
     * An iterable view on the user meta entries.
     * 
     * @return an iterable view of the set of user meta data.
     * @see Entry
     */
    Iterable<Entry<String, String>> userMetaEntries();

    // Mutate
    /**
     * Set this IRiakObject's value.
     * 
     * @param value
     *            the byte[] to set.
     */
    void setValue(byte[] value);

    /**
     * Convenience method that will result in value being turned into a byte[]
     * array using charset utf-8 and also will result in charset=utf-8 being
     * appended to the content-type for this object
     * 
     * @param value
     *            the String value
     */
    void setValue(String value);

    /**
     * Set the content-type of this object's payload.
     * 
     * @param contentType
     *            the content-type of this object's value (EG
     *            text/plain;charset=utf-8)
     */
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
     * Adds the key, value to the collection of user meta data for this object.
     * 
     * @param key
     * @param value
     * @return this RiakObject.
     */
    IRiakObject addUsermeta(String key, String value);

    /**
     * Remove that item of user meta data.
     * 
     * @param key
     *            the key of the item to remove
     */
    IRiakObject removeUsermeta(String key);
}
