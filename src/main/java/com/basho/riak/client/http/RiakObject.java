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

import static com.basho.riak.client.util.CharsetUtils.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;
import com.basho.riak.client.http.response.StoreResponse;
import com.basho.riak.client.http.response.WalkResponse;
import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;

/**
 * A Riak object.
 */
public class RiakObject {

    private static final byte[] EMPTY = new byte[] {};

    private RiakClient riak;
    private String bucket;
    private String key;
    private byte[] value;
    private List<RiakLink> links;
    private final Object indexLock = new Object();
    @SuppressWarnings("rawtypes") private List<RiakIndex> indexes;
    private Map<String, String> userMetaData;
    private String contentType;
    private String vclock;
    private String lastmod;
    private String vtag;
    private InputStream valueStream;
    private Long valueStreamLength;

    /**
     * Create an object. The content type defaults to
     * application/octet-stream.
     * 
     * @param riak
     *            Riak instance this object is associated with, which is used by
     *            the convenience methods in this class (e.g.
     *            {@link RiakObject#store()}).
     * @param bucket
     *            The object's bucket
     * @param key
     *            The object's key
     * @param value
     *            The object's value
     * @param contentType
     *            The object's content type which defaults to
     *            application/octet-stream if null.
     * @param links
     *            Links to other objects
     * @param userMetaData
     *            Custom metadata key-value pairs for this object
     * @param vclock
     *            An opaque vclock assigned by Riak
     * @param lastmod
     *            The last time this object was modified according to Riak
     * @param vtag
     *            This object's entity tag assigned by Riak
     */
    public RiakObject(RiakClient riak, String bucket, String key, byte[] value, String contentType,
            List<RiakLink> links, Map<String, String> userMetaData, String vclock, String lastmod, String vtag, @SuppressWarnings("rawtypes") List<RiakIndex> indexes) {
        this.riak = riak;
        this.bucket = bucket;
        this.key = key;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;

        safeSetValue(value);
        this.contentType = contentType == null ? Constants.CTYPE_OCTET_STREAM : contentType;
        safeSetLinks(links);
        safeSetUsermetaData(userMetaData);
        safeSetIndexes(indexes);
    }

    /**
     * Create an empty object. The content type defaults to
     * application/octet-stream.
     * 
     * @param riak
     *            Riak instance this object is associated with, which is used by
     *            the convenience methods in this class (e.g.
     *            {@link RiakObject#store()}).
     * @param bucket
     *            The object's bucket
     * @param key
     *            The object's key
     * @param value
     *            The object's value
     * @param contentType
     *            The object's content type which defaults to
     *            application/octet-stream if null.
     * @param links
     *            Links to other objects
     * @param userMetaData
     *            Custom metadata key-value pairs for this object
     * @param vclock
     *            An opaque vclock assigned by Riak
     * @param lastmod
     *            The last time this object was modified according to Riak
     * @param vtag
     *            This object's entity tag assigned by Riak
     */
    public RiakObject(RiakClient riak, String bucket, String key, byte[] value, String contentType,
            List<RiakLink> links, Map<String, String> userMetaData, String vclock, String lastmod, String vtag) {
       this(riak, bucket, key, value, contentType, links, userMetaData, vclock, lastmod, vtag, null);
    }

    public RiakObject(RiakClient riak, String bucket, String key) {
        this(riak, bucket, key, null, null, null, null, null, null, null);
    }

    public RiakObject(RiakClient riak, String bucket, String key, byte[] value) {
        this(riak, bucket, key, value, null, null, null, null, null, null);
    }

    public RiakObject(RiakClient riak, String bucket, String key, byte[] value, String contentType) {
        this(riak, bucket, key, value, contentType, null, null, null, null, null);
    }

    public RiakObject(RiakClient riak, String bucket, String key, byte[] value, String contentType, List<RiakLink> links) {
        this(riak, bucket, key, value, contentType, links, null, null, null, null);
    }

    public RiakObject(RiakClient riak, String bucket, String key, byte[] value, String contentType,
            List<RiakLink> links, Map<String, String> userMetaData) {
        this(riak, bucket, key, value, contentType, links, userMetaData, null, null, null);
    }

    public RiakObject(String bucket, String key) {
        this(null, bucket, key, null, null, null, null, null, null, null);
    }

    public RiakObject(String bucket, String key, byte[] value) {
        this(null, bucket, key, value, null, null, null, null, null, null);
    }

    public RiakObject(String bucket, String key, byte[] value, String contentType) {
        this(null, bucket, key, value, contentType, null, null, null, null, null);
    }

    public RiakObject(String bucket, String key, byte[] value, String contentType, List<RiakLink> links) {
        this(null, bucket, key, value, contentType, links, null, null, null, null);
    }

    public RiakObject(String bucket, String key, byte[] value, String contentType, List<RiakLink> links,
            Map<String, String> userMetaData) {
        this(null, bucket, key, value, contentType, links, userMetaData, null, null, null);
    }

    public RiakObject(String bucket, String key, byte[] value, String contentType, List<RiakLink> links,
            Map<String, String> userMetaData, String vclock, String lastmod, String vtag) {
        this(null, bucket, key, value, contentType, links, userMetaData, vclock, lastmod, vtag);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getRiakClient()
     */
    public RiakClient getRiakClient() {
        return riak;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setRiakClient(com.basho.riak.client.RiakClient)
     */
    public RiakObject setRiakClient(RiakClient client) {
        riak = client;
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#copyData(com.basho.riak.client.RiakObject)
     */
    public void copyData(RiakObject object) {
        if (object == null)
            return;

        if (object.value != null) {
            value = object.value.clone();
        } else {
            value = null;
        }

        valueStream = object.valueStream;
        valueStreamLength = object.valueStreamLength;

        setLinks(object.links);

        userMetaData = new HashMap<String, String>();
        if (object.userMetaData != null) {
            userMetaData.putAll(object.userMetaData);
        }
        contentType = object.contentType;
        vclock = object.vclock;
        lastmod = object.lastmod;
        vtag = object.vtag;
    }

    /**
     * Perform a shallow copy of the object
     */
    void shallowCopy(RiakObject object) {
        value = object.value;
        this.links = object.links;
        userMetaData = object.userMetaData;
        contentType = object.contentType;
        vclock = object.vclock;
        lastmod = object.lastmod;
        vtag = object.vtag;
        valueStream = object.valueStream;
        valueStreamLength = object.valueStreamLength;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#updateMeta(com.basho.riak.client.response.StoreResponse)
     */
    public void updateMeta(StoreResponse response) {
        if (response == null) {
            vclock = null;
            lastmod = null;
            vtag = null;
        } else {
            vclock = response.getVclock();
            lastmod = response.getLastmod();
            vtag = response.getVtag();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#updateMeta(com.basho.riak.client.response.FetchResponse)
     */
    public void updateMeta(FetchResponse response) {
        if (response == null || response.getObject() == null) {
            vclock = null;
            lastmod = null;
            vtag = null;
        } else {
            vclock = response.getObject().getVclock();
            lastmod = response.getObject().getLastmod();
            vtag = response.getObject().getVtag();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getBucket()
     */
    public String getBucket() {
        return bucket;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getKey()
     */
    public String getKey() {
        return key;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getValue()
     */
    public String getValue() {
        return (value == null ? null : asString(value, getCharset(contentType)));
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getValueAsBytes()
     */
    public byte[] getValueAsBytes() {
        return value == null ? value : value.clone();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setValue(java.lang.String)
     */
    public void setValue(String value) {
        if (value != null) {
            this.value = asBytes(value, getCharset(contentType));
        } else {
            this.value = null;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setValue(byte[])
     */
    public void setValue(byte[] value) {
       safeSetValue(value);
    }

    /**
     *
     * @param value
     */
    private void safeSetValue(final byte[] value) {
        if(value != null) {
            this.value = value.clone();
        } else {
            this.value = null;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setValueStream(java.io.InputStream, java.lang.Long)
     */
    public void setValueStream(InputStream in, Long len) {
        valueStream = in;
        valueStreamLength = len;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setValueStream(java.io.InputStream)
     */
    public void setValueStream(InputStream in) {
        valueStream = in;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getValueStream()
     */
    public InputStream getValueStream() {
        return valueStream;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setValueStreamLength(java.lang.Long)
     */
    public void setValueStreamLength(Long len) {
        valueStreamLength = len;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getValueStreamLength()
     */
    public Long getValueStreamLength() {
        return valueStreamLength;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getLinks()
     */
    @Deprecated
    public List<RiakLink> getLinks() {
        return this.links;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setLinks(java.util.List)
     */
    public void setLinks(List<RiakLink> links) {
       safeSetLinks(links);
    }

    private void safeSetLinks(final List<RiakLink> links) {
        if (links == null) {
            this.links = new CopyOnWriteArrayList<RiakLink>();
        } else {
            this.links = new CopyOnWriteArrayList<RiakLink>(deepCopy(links));
        }
    }

    @SuppressWarnings("rawtypes") private void safeSetIndexes(final List<RiakIndex> indexes) {
        if (indexes == null) {
            this.indexes = new ArrayList<RiakIndex>();
        } else {
            this.indexes = new ArrayList<RiakIndex>(indexes);
        }
    }

    /**
     * Creates a new RiakLink for each RiakLink in links and adds it to a new List.
     *
     * @param links a List of {@link RiakLink}s
     * @return a deep copy of List.
     */
    private List<RiakLink> deepCopy(List<RiakLink> links) {
        final ArrayList<RiakLink> copyLinks = new ArrayList<RiakLink>();

        for(RiakLink link : links) {
            copyLinks.add(new RiakLink(link));
        }

        return copyLinks;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#addLink(com.basho.riak.client.RiakLink)
     */
    public RiakObject addLink(RiakLink link) {
        if (link != null) {
            links.add(link);
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#removeLink(com.basho.riak.client.RiakLink)
     */
    public RiakObject removeLink(final RiakLink link) {
        this.links.remove(link);
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#hasLinks()
     */
    public boolean hasLinks() {
        return !links.isEmpty();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#numLinks()
     */
    public int numLinks() {
        return links.size();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#hasLink(com.basho.riak.client.RiakLink)
     */
    public boolean hasLink(final RiakLink riakLink) {
        return links.contains(riakLink);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getUsermeta()
     */
    @Deprecated
    public Map<String, String> getUsermeta() {
        return userMetaData;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setUsermeta(java.util.Map)
     */
    public void setUsermeta(final Map<String, String> userMetaData) {
       safeSetUsermetaData(userMetaData);
    }

    private void safeSetUsermetaData(final Map<String, String> userMetaData) {
        if (userMetaData == null) {
            this.userMetaData = new ConcurrentHashMap<String, String>();
        } else {
            this.userMetaData = new ConcurrentHashMap<String, String>(userMetaData);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#addUsermetaItem(java.lang.String, java.lang.String)
     */
    public RiakObject addUsermetaItem(String key, String value) {
        userMetaData.put(key, value);
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#hasUsermeta()
     */
    public boolean hasUsermeta() {
        return !userMetaData.isEmpty();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#numUsermetaItems()
     */
    public int numUsermetaItems() {
        return userMetaData.size();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#hasUsermetaItem(java.lang.String)
     */
    public boolean hasUsermetaItem(String key) {
        return userMetaData.containsKey(key);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getUsermetaItem(java.lang.String)
     */
    public String getUsermetaItem(String key) {
        return userMetaData.get(key);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#removeUsermetaItem(java.lang.String)
     */
    public void removeUsermetaItem(String key) {
        userMetaData.remove(key);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#usermetaKeys()
     */
    public Iterable<String> usermetaKeys() {
        return userMetaData.keySet();
    }

    /**
     * @return a *copy* of the list of {@link RiakIndex}es for this object
     */
    @SuppressWarnings("rawtypes") public List<RiakIndex> getIndexes() {
        synchronized (indexLock) {
            return new ArrayList<RiakIndex>(indexes);
        }
    }

    /**
     * Add a binary index to the object
     * 
     * @param name
     *            of the index
     * @param value
     *            the value to add to the index
     * @return this
     */
    public RiakObject addIndex(String name, String value) {
        synchronized (indexLock) {
            indexes.add(new BinIndex(name, value));
        }
        return this;
    }

    /**
     * Add an int index to this object
     * 
     * @param name
     *            of the index
     * @param value
     *            the value to add to the index
     * @return this
     */
    public RiakObject addIndex(String name, int value) {
        synchronized (indexLock) {
            indexes.add(new IntIndex(name, value));
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getContentType()
     */
    public String getContentType() {
        return contentType;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#setContentType(java.lang.String)
     */
    public void setContentType(String contentType) {
        if (contentType != null) {
            this.contentType = contentType;
        } else {
            this.contentType = Constants.CTYPE_OCTET_STREAM;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getVclock()
     */
    public String getVclock() {
        return vclock;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getLastmod()
     */
    public String getLastmod() {
        return lastmod;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getLastmodAsDate()
     */
    public Date getLastmodAsDate() {
        try {
            return DateUtils.parseDate(lastmod);
        } catch (DateParseException e) {
            return null;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#getVtag()
     */
    public String getVtag() {
        return vtag;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#store(com.basho.riak.client.request.RequestMeta)
     */
    public StoreResponse store(RequestMeta meta) {
        return store(riak, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#store()
     */
    public StoreResponse store() {
        return store(riak, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#store(com.basho.riak.client.RiakClient, com.basho.riak.client.request.RequestMeta)
     */
    public StoreResponse store(RiakClient riak, RequestMeta meta) {
        if (riak == null)
            throw new IllegalStateException("Cannot store an object without a RiakClient");

        StoreResponse r = riak.store(this, meta);
        if (r.isSuccess()) {
            this.updateMeta(r);
        }
        return r;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#fetch(com.basho.riak.client.request.RequestMeta)
     */
    public FetchResponse fetch(RequestMeta meta) {
        if (riak == null)
            throw new IllegalStateException("Cannot fetch an object without a RiakClient");

        FetchResponse r = riak.fetch(bucket, key, meta);
        if (r.getObject() != null) {
            RiakObject other = r.getObject();
            shallowCopy(other);
            r.setObject(this);
        }
        return r;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#fetch()
     */
    public FetchResponse fetch() {
        return fetch(null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#fetchMeta(com.basho.riak.client.request.RequestMeta)
     */
    public FetchResponse fetchMeta(RequestMeta meta) {
        if (riak == null)
            throw new IllegalStateException("Cannot fetch meta for an object without a RiakClient");

        FetchResponse r = riak.fetchMeta(bucket, key, meta);
        if (r.isSuccess()) {
            this.updateMeta(r);
        }
        return r;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#fetchMeta()
     */
    public FetchResponse fetchMeta() {
        return fetchMeta(null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#delete(com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse delete(RequestMeta meta) {
        if (riak == null)
            throw new IllegalStateException("Cannot delete an object without a RiakClient");

        return riak.delete(bucket, key, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#delete()
     */
    public HttpResponse delete() {
        return delete(null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk(java.lang.String, java.lang.String, boolean)
     */
    public LinkBuilder walk(String bucket, String tag, boolean keep) {
        return new LinkBuilder().walk(bucket, tag, keep);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk(java.lang.String, java.lang.String)
     */
    public LinkBuilder walk(String bucket, String tag) {
        return new LinkBuilder().walk(bucket, tag);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk(java.lang.String, boolean)
     */
    public LinkBuilder walk(String bucket, boolean keep) {
        return new LinkBuilder().walk(bucket, keep);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk(java.lang.String)
     */
    public LinkBuilder walk(String bucket) {
        return new LinkBuilder().walk(bucket);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk()
     */
    public LinkBuilder walk() {
        return new LinkBuilder().walk();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#walk(boolean)
     */
    public LinkBuilder walk(boolean keep) {
        return new LinkBuilder().walk(keep);
    }

    /**
     * Serializes this object to an existing {@link HttpRequestBase} which can
     * be sent as an HTTP request. Specifically, sends the object's link,
     * user-defined metadata and vclock as HTTP headers and the value as the
     * body. Used by {@link RiakClient} to create PUT requests.
     * 
     * if the this RiakObject's value is a stream, and no length is set, the
     * stream is first buffered into a byte array before being written
     */
    public void writeToHttpMethod(HttpRequestBase httpMethod) {
        // Serialize headers
        String basePath = getBasePathFromHttpMethod(httpMethod);
        writeLinks(httpMethod, basePath);
        for (String name : userMetaData.keySet()) {
            httpMethod.addHeader(Constants.HDR_USERMETA_REQ_PREFIX + name, userMetaData.get(name));
        }

        writeIndexes(httpMethod);

        if (vclock != null) {
            httpMethod.addHeader(Constants.HDR_VCLOCK, vclock);
        }

        // Serialize body
        if (httpMethod instanceof HttpEntityEnclosingRequestBase) {
            HttpEntityEnclosingRequestBase entityEnclosingMethod = (HttpEntityEnclosingRequestBase) httpMethod;
            AbstractHttpEntity entity = null;
            // Any value set using setValueAsStream() has precedent over value
            // set using setValue()
            if (valueStream != null) {
                if (valueStreamLength != null && valueStreamLength >= 0) {
                    entity = new InputStreamEntity(valueStream, valueStreamLength);
                } else {
                    // since apache http client 4.1 no longer supports buffering stream entities, but we can't change API
                    // behaviour, here we have to buffer the whole content
                    entity = new ByteArrayEntity(ClientUtils.bufferStream(valueStream));
                }
            } else if (value != null) {
                entity = new ByteArrayEntity(value);
            } else {
                entity = new ByteArrayEntity(EMPTY);
            }
            entity.setContentType(contentType);
            entityEnclosingMethod.setEntity(entity);
        }
    }

    /**
     * Write HTTP header for each secondary index
     * @param httpMethod
     */
    @SuppressWarnings("rawtypes") private void writeIndexes(HttpRequestBase httpMethod) {
        // write 2i headers
        synchronized (indexLock) {
            for (RiakIndex i : indexes) {
                String index = i.getName();
                httpMethod.addHeader(Constants.HDR_SEC_INDEX_REQ_PREFIX + index, i.getValue().toString());
            }
        }
    }

    private void writeLinks(HttpRequestBase httpMethod, String basePath) {
        StringBuilder linkHeader = new StringBuilder();

        for (RiakLink link : this.links) {
            if (linkHeader.length() > 0) {
                linkHeader.append(", ");
            }
            linkHeader.append("<");
            linkHeader.append(basePath);
            linkHeader.append("/");
            linkHeader.append(link.getBucket());
            linkHeader.append("/");
            linkHeader.append(link.getKey());
            linkHeader.append(">; ");
            linkHeader.append(Constants.LINK_TAG);
            linkHeader.append("=\"");
            linkHeader.append(link.getTag());
            linkHeader.append("\"");

            // To avoid (MochiWeb) problems with too long headers, flush if
            // it grows too big:
            if (linkHeader.length() > 2000) {
                httpMethod.addHeader(Constants.HDR_LINK, linkHeader.toString());
                linkHeader = new StringBuilder();
            }
        }
        if (linkHeader.length() > 0) {
            httpMethod.addHeader(Constants.HDR_LINK, linkHeader.toString());
        }
    }

    String getBasePathFromHttpMethod(HttpRequestBase httpMethod) {
        if (httpMethod == null || httpMethod.getURI() == null)
            return "";

        String path = httpMethod.getURI().getRawPath();
        int idx = path.length() - 1;

        // ignore any trailing slash
        if (path.endsWith("/")) {
            idx--;
        }

        // trim off last two path components
        idx = path.lastIndexOf('/', idx);
        idx = path.lastIndexOf('/', idx - 1);

        if (idx <= 0)
            return "";

        return path.substring(0, idx);
    }

    /**
     * Created by links() as a convenient way to build up link walking queries
     */
    public class LinkBuilder {

        private RiakWalkSpec walkSpec = new RiakWalkSpec();

        public LinkBuilder walk() {
            walkSpec.addStep(RiakWalkSpec.WILDCARD, RiakWalkSpec.WILDCARD);
            return this;
        }

        public LinkBuilder walk(boolean keep) {
            walkSpec.addStep(RiakWalkSpec.WILDCARD, RiakWalkSpec.WILDCARD, keep);
            return this;
        }

        public LinkBuilder walk(String bucket) {
            walkSpec.addStep(bucket, RiakWalkSpec.WILDCARD);
            return this;
        }

        public LinkBuilder walk(String bucket, boolean keep) {
            walkSpec.addStep(bucket, RiakWalkSpec.WILDCARD, keep);
            return this;
        }

        public LinkBuilder walk(String bucket, String tag) {
            walkSpec.addStep(bucket, tag);
            return this;
        }

        public LinkBuilder walk(String bucket, String tag, boolean keep) {
            walkSpec.addStep(bucket, tag, keep);
            return this;
        }

        public String getWalkSpec() {
            return walkSpec.toString();
        }

        /**
         * Execute the link walking query by calling
         * {@link RiakClient#walk(String, String, String, RequestMeta)}.
         * 
         * @param meta
         *            Extra metadata to attach to the request such as HTTP
         *            headers or query parameters.
         * @return See
         *         {@link RiakClient#walk(String, String, String, RequestMeta)}.
         * 
         * @throws RiakIORuntimeException
         *             If an error occurs during communication with the Riak
         *             server.
         * @throws RiakResponseRuntimeException
         *             If the Riak server returns a malformed response.
         */
        public WalkResponse run(RequestMeta meta) {
            if (riak == null)
                throw new IllegalStateException("Cannot perform object link walk without a RiakClient");
            return riak.walk(bucket, key, getWalkSpec(), meta);
        }

        public WalkResponse run() {
            return run(null);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakObject#iterableLinks()
     */
    public Iterable<RiakLink> iterableLinks() {
        return new Iterable<RiakLink>() {
            public Iterator<RiakLink> iterator() {
                return links.iterator();
            }
        };
    }
}