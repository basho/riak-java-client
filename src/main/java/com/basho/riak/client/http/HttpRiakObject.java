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

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.http.RiakObject.LinkBuilder;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.StoreResponse;

/**
 * @author russell
 *
 */
public interface HttpRiakObject {

    /**
     * A {@link RiakObject} can be loosely attached to the {@link RiakClient}
     * from which retrieve it was retrieved. Calling convenience methods like
     * {@link RiakObject#store()} will store this object use that client.
     */
    RiakClient getRiakClient();

    RiakObject setRiakClient(RiakClient client);

    /**
     * Copy the metadata and value from <code>object</code>. The bucket and key
     * are not copied.
     * 
     * @param object
     *            The source object to copy from
     */
    void copyData(RiakObject object);

    /**
     * Update the object's metadata. This usually happens when Riak returns
     * updated metadata from a store operation.
     * 
     * @param response
     *            Response from a store operation containing an updated vclock,
     *            last modified date, and vtag
     */
    void updateMeta(StoreResponse response);

    /**
     * Update the object's metadata from a fetch or fetchMeta operation
     * 
     * @param response
     *            Response from a fetch or fetchMeta operation containing a
     *            vclock, last modified date, and vtag
     */
    void updateMeta(FetchResponse response);

    /**
     * The object's bucket
     */
    String getBucket();

    /**
     * The object's key
     */
    String getKey();

    /**
     * The object's value
     */
    String getValue();

    byte[] getValueAsBytes();

    void setValue(String value);

    void setValue(byte[] value);

    /**
     * Set the object's value as a stream. A value set here is independent of
     * and has precedent over any value set using setValue():
     * {@link RiakObject#writeToHttpMethod(HttpMethod)} will always write the
     * value from getValueStream() if it is not null. Calling getValue() will
     * always return values set via setValue(), and calling getValueStream()
     * will always return the stream set via setValueStream.
     * 
     * @param in
     *            Input stream representing the object's value
     * @param len
     *            Length of the InputStream or null if unknown. If null, the
     *            value will be buffered in memory to determine its size before
     *            sending to the server.
     */
    void setValueStream(InputStream in, Long len);

    void setValueStream(InputStream in);

    InputStream getValueStream();

    void setValueStreamLength(Long len);

    Long getValueStreamLength();

    /**
     * The object's links -- may be empty, but never be null.
     *
     * @see {@link RiakObject#addLink()}, {@link RiakObject#removeLink()}, {@link RiakObject#iterator()}, {@link RiakObject#hasLinks()} and , {@link RiakObject#numLinks()}
     *
     * @return the list of {@link RiakLink}s for this
     *         RiakObject
     * @deprecated please use {@link RiakObject#iterableLinks())} to iterate over the
     *             collection of {@link RiakLink}s. Attempting to mutate the
     *             collection will result in UnsupportedOperationException in
     *             future versions. Use {@link RiakObject#addLink()} and {@link RiakObject#removeLink()} instead.
     *             Use {@link RiakObject#hasLinks()}, {@link RiakObject#numLinks()} and {@link RiakObject#hasLink(RiakLink)}
     *             to query state of links.
     */
    @Deprecated List<RiakLink> getLinks();

    /**
     * Makes a *deep* copy of links.
     *
     * Changes made to the original collection and its contents will not be reflected
     * in this RiakObject's links. Use {@link RiakObject#addLink(RiakLink)},
     * {@link RiakObject#removeLink(RiakLink)} and {@link RiakObject#setLinks(List)} to alter the collection.
     * @param links a List of {@link RiakLink}
     */
    void setLinks(List<RiakLink> links);

    /**
     * Add link to this RiakObject's links.
     * @param link a {@link RiakLink} to add.
     * @return this RiakObject.
     */
    RiakObject addLink(RiakLink link);

    /**
     * Remove a {@link RiakLink} from this RiakObject.
     * @param link the {@link RiakLink} to remove
     * @return this RiakObject
     */
    RiakObject removeLink(final RiakLink link);

    /**
     * Does this RiakObject have any {@link RiakLink}s?
     * @return true if there are links, false otherwise
     */
    boolean hasLinks();

    /**
     * How many {@link RiakLink}s does this RiakObject have?
     * @return the number of {@link RiakLink}s this object has.
     */
    int numLinks();

    /**
     * Checks if the collection of RiakLinks contains the one passed in.
     * @param riakLink a RiakLink
     * @return true if the RiakObject's link collection contains riakLink.
     */
    boolean hasLink(final RiakLink riakLink);

    /**
     * User-specified metadata for the object in the form of key-value pairs --
     * may be empty, but never be null. New key-value pairs can be added using
     * addUsermeta()
     *
     * @deprecated Future versions will return an unmodifiable view of the user meta. Please use
     *             {@link RiakObject#addUsermeta(String, String)},
     *             {@link RiakObject#removeUsermetaItem(String)},
     *             {@link RiakObject#setUsermeta(Map)},
     *             {@link RiakObject#hasUsermetaItem(String)},
     *             {@link RiakObject#hasUsermeta()} and
     *             {@link RiakObject#getUsermetaItem(String)} to mutate and query the User meta collection
     */
    @Deprecated Map<String, String> getUsermeta();

    /**
     * Creates a copy of userMetaData. Changes made to the original collection will not be
     * reflected in the RiakObject's state.
     * @param userMetaData
     */
    void setUsermeta(final Map<String, String> userMetaData);

    /**
     * Adds the key, value to the collection of user meta for this object.
     * @param key
     * @param value
     * @return this RiakObject.
     */
    RiakObject addUsermetaItem(String key, String value);

    /**
     * @return true if there are any user meta data set on this RiakObject.
     */
    boolean hasUsermeta();

    /**
     * @return how many user meta data items this RiakObject has.
     */
    int numUsermetaItems();

    /**
     * @param key
     * @return
     */
    boolean hasUsermetaItem(String key);

    /**
     * Get an item of user meta data.
     * @param key the user meta data item key
     * @return The value for the given key or null.
     */
    String getUsermetaItem(String key);

    /**
     * @param key the key of the item to remove
     */
    void removeUsermetaItem(String key);

    Iterable<String> usermetaKeys();

    /**
     * The object's content type as a MIME type
     */
    String getContentType();

    void setContentType(String contentType);

    /**
     * The object's opaque vclock assigned by Riak
     */
    String getVclock();

    /**
     * The modification date of the object determined by Riak
     */
    String getLastmod();

    /**
     * Convenience method to get the last modified header parsed into a Date
     * object. Returns null if header is null, malformed, or cannot be parsed.
     */
    Date getLastmodAsDate();

    /**
     * An entity tag for the object assigned by Riak
     */
    String getVtag();

    /**
     * Convenience method for calling
     * {@link RiakClient#store(RiakObject, RequestMeta)} followed by
     * {@link RiakObject#updateMeta(StoreResponse)}
     * 
     * @throws IllegalStateException
     *             if this object was not fetched from a Riak instance, so there
     *             is not associated server to store it with.
     */
    StoreResponse store(RequestMeta meta);

    StoreResponse store();

    /**
     * Store this object to a different Riak instance.
     * 
     * @param riak
     *            Riak instance to store this object to
     * @param meta
     *            Same as {@link RiakClient#store(RiakObject, RequestMeta)}
     * @throws IllegalStateException
     *             if this object was not fetched from a Riak instance, so there
     *             is not associated server to store it with.
     */
    StoreResponse store(RiakClient riak, RequestMeta meta);

    /**
     * Convenience method for calling {@link RiakClient#fetch(String, String)}
     * followed by {@link RiakObject#copyData(RiakObject)}
     * 
     * @param meta
     *            Same as {@link RiakClient#fetch(String, String, RequestMeta)}
     * @throws IllegalStateException
     *             if this object was not fetched from a Riak instance, so there
     *             is not associated server to refetch it from.
     */
    FetchResponse fetch(RequestMeta meta);

    FetchResponse fetch();

    /**
     * Convenience method for calling
     * {@link RiakClient#fetchMeta(String, String, RequestMeta)} followed by
     * {@link RiakObject#updateMeta(FetchResponse)}
     * 
     * @throws IllegalStateException
     *             if this object was not fetched from a Riak instance, so there
     *             is not associated server to refetch meta from.
     */
    FetchResponse fetchMeta(RequestMeta meta);

    FetchResponse fetchMeta();

    /**
     * Convenience method for calling
     * {@link RiakClient#delete(String, String, RequestMeta)}.
     * 
     * @throws IllegalStateException
     *             if this object was not fetched from a Riak instance, so there
     *             is not associated server to delete from.
     */
    HttpResponse delete(RequestMeta meta);

    HttpResponse delete();

    /**
     * Convenience methods for building a link walk specification starting from
     * this object and calling
     * {@link RiakClient#walk(String, String, RiakWalkSpec)}
     * 
     * @param bucket
     *            The bucket to follow object links to
     * @param tag
     *            The link tags to follow from this object
     * @param keep
     *            Whether to keep the output from this link walking step. If not
     *            specified, then the output is only kept from the last step.
     * @return A {@link LinkBuilder} object to continue building the walk query
     *         or to run it.
     */
    LinkBuilder walk(String bucket, String tag, boolean keep);

    LinkBuilder walk(String bucket, String tag);

    LinkBuilder walk(String bucket, boolean keep);

    LinkBuilder walk(String bucket);

    LinkBuilder walk();

    LinkBuilder walk(boolean keep);

    /**
     * Serializes this object to an existing {@link HttpMethod} which can be
     * sent as an HTTP request. Specifically, sends the object's link,
     * user-defined metadata and vclock as HTTP headers and the value as the
     * body. Used by {@link RiakClient} to create PUT requests.
     */
    void writeToHttpMethod(HttpMethod httpMethod);

    /**
     * A thread safe, snapshot Iterable view of the state of this RiakObject's {@link RiakLink}s at call time.
     * Modifications are *NOT* supported.
     * @return Iterable<RiakLink> for this RiakObject's {@link RiakLink}s
     */
    Iterable<RiakLink> iterableLinks();

}