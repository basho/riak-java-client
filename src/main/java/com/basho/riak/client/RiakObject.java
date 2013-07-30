/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the data and meta data stored in Riak at a bucket/key.
 * 
 * <p>
 * Although you can store your own Java Beans in Riak, this interface represents
 * the core data type that is passed between the low and high level APIs and
 * that all POJOs are converted to and from when stored or fetched.
 * </p>
 * 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @author Russel Brown <russelldb at basho dot com>
 * @since 2.0
 */
public interface RiakObject
{
    /**
     * The name of this objects bucket
     * 
     * @return the bucket name as raw bytes.
     */
    ByteBuffer getBucket();

    /**
     * The name of this Objects bucket. 
     * 
     * Riak stores bucket names as bytes. If the bytes do not represent a
     * UTF-8 String your mileage may vary.
     * 
     * @return The bucket name as a UTF-8 String. 
     */
    String getBucketAsString();
    
    /**
     * The name of this Objects bucket. 
     * 
     * Riak stores bucket names as bytes. This returns those bytes encoded as a
     * String using the supplied Charset.
     * 
     * @param charset the encoding to use
     * @return The bucket name as a String
     */
    String getBucketAsString(Charset charset) throws UnsupportedEncodingException;
    
    /**
     * The value.
     * 
     * @return byte[] of this object value or {@code null} if no value has been set.
     */
    ByteBuffer getValue();

    /**
     * Convenience method. The value will be coerced to a {@code String} using the 
     * object's {@code Charset} determined from the content type. 
     * 
     * @return the byte[] coerced to a {@code String} using the object's {@code Charset} 
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
     * @return The objects key as raw bytes. 
     */
    ByteBuffer getKey();
    
    /**
     * The Object's key. 
     * 
     * Riak stores keys as bytes. If the bytes do not represent a
     * UTF-8 String your mileage may vary.
     * 
     * @return The key as a UTF-8 String. 
     */
    String getKeyAsString();
    
    /**
     * The Object's key. 
     * 
     * Riak stores keys as bytes. This returns those bytes encoded as a
     * String using the supplied Charset.
     * 
     * @param charset the encoding to use
     * @return The key as a String
     */
    String getKeyAsString(Charset charset) throws UnsupportedEncodingException;
    
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
     * The {@code Charset} for this object's value. 
     * 
     * @return the object's Charset. If this can not be determined from the 
     * content type {@code null} is 
     * returned
     */
    String getCharset();
    
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
     * key/value pairs that are stored with the RiakObject riak.
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
     * @return if this RiakObject has any user meta data items.
     */
    boolean hasUsermeta();

    /**
     * Does this object have a meta data item for that key?
     * 
     * @param key
     * @return true if this RiakObject's user meta data contains the supplied
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
     */
    Iterable<Map.Entry<String, String>> userMetaEntries();

    /**
     * Does this object have any secondary indexes?
     * @return true if this RiakObject has secondary indexes 
     */
    boolean hasIndexes();
    
    /**
     * Does this RiakObject have this {@link IntIndex}
     * 
     * @param name the name of the IntIndex
     * @return true if it has it
     */
    boolean hasIntIndex(String name);
    
    /**
     * Does this RiakObject have this {@link BinIndex}
     * 
     * @param name the name of the BinIndex
     * @return true if it has it
     */
    boolean hasBinIndex(String name);
    
    /**
     * Secondary indexes for this object.
     * 
     * See <a
     * href="http://docs.basho.com/riak/latest/tutorials/querying/Secondary-Indexes/">basho
     * docs</a> for more details.
     *
     * @return a copy of the string indexes for this object.
     */
    Map<BinIndex, Set<String>> allBinIndexes();

    /**
     * Get a copy of the values for the given bin index
     * @param string the index name
     * @return a Set of value
     */
    Set<String> getBinIndex(String string);

    /**
     * Secondary indexes for this object.
     * 
     * See <a
     * href="http://docs.basho.com/riak/latest/tutorials/querying/Secondary-Indexes/">basho
     * Docs</a> for more details.
     * 
     * @return a copy of the int_ indexes for this object
     */
    Map<IntIndex, Set<Long>> allIntIndexes();
	
    /**
     * Get a copy of the values for the given int_ index
     * @param string the index name
     * @return a Set of value
     */
    Set<Long> getIntIndex(String string);
	
    // Mutate
    /**
     * Set this RiakObject's value.
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
     * Convenience method that will result in value being turned into a byte[]
     * array using the provided {@code Charset}. The charset will also be sent
     * to Riak during a store operation.
     * 
     * 
     * @param value the String value
     * @param charset the charset encoding for this value
     */
    void setValue(String value, Charset charset) throws UnsupportedEncodingException;
    
    /**
     * Set the content-type of this object's payload.
     * 
     * @param contentType
     *            the content-type of this object's value (EG
     *            text/plain;charset=utf-8)
     */
    void setContentType(String contentType);

    /**
     * Set the {@code Charset} of this object's payload. This is added
     * to the content type. If a charset is already present it is replaced.
     * 
     * @param charset the {@code Charset} of the object's payload. 
     * @see #getValueAsString() 
     */
    void setCharset(Charset charset);
    
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
     * Adds the key, value to the collection of user meta data for this object.
     * 
     * @param key
     * @param value
     * @return this RiakObject.
     */
    RiakObject addUsermeta(String key, String value);

    /**
     * Remove that item of user meta data.
     * 
     * @param key
     *            the key of the item to remove
     */
    RiakObject removeUsermeta(String key);

    /**
     * Add an index value to a {@link BinIndex}. If the index does
     * not currently exist it will be created.
     * 
     * @param index
     *            index name
     * @param value
     *            index value
     * @return this RiakObject.
     */
    RiakObject addIndex(String index, String value);

    /**
     * Add an index value to an {@link IntIndex}. If the index
     * does not currently exist it will be created.
     * 
     * @param index
     *            index name
     * @param value
     *            index value
     * @return this RiakObject.
     */
    RiakObject addIndex(String index, long value);

    /**
     * Remove a {@link BinIndex} from this RiakObject.
     * 
     * @param index
     *            the name of the bin index to remove
     * @return this RiakObject
     */
    RiakObject removeBinIndex(String index);

    /**
     * Remove a {@link IntIndex} from this RiakObject.
     * 
     * @param index
     *            the name of the int index to remove
     * @return this RiakObject
     */
    RiakObject removeIntIndex(String index);
    
    /**
     * Remove an index value from the specified {@link BinIndex}
     * @param indexName the name of the index
     * @param value the value to remove
     * @return this
     */
    RiakObject removeFromBinIndex(String indexName, String value);
    
    /**
     * Remove an index value from the specified {@link IntIndex}
     * @param indexName the name of the index
     * @param value the value to remove
     * @return this
     */
    RiakObject removeFromIntIndex(String indexName, long value);
    
    /**
     * Check to see if this object is a tombstone (deleted)
     * 
     * Note: The request has to have been made specifying tombstones
     * (deleted vclocks) are to be returned.  
     * @return true if the object is a tombstone
     */
    boolean isDeleted();
    
    /**
     * Check to see if this object is modified.
     * 
     * This will return true unless this object represents the result of a conditional
     * fetch where the object had not been modified since the supplied constraint
     * 
     * @see FetchMeta.Builder#modifiedSince
     * @return true unless the conditional fetch returned no object
     */
    boolean isModified();
    
    /**
     * Check to see if this object was not found
     * @return true unless the result of a fetch was not found
     */
    boolean notFound();
}
