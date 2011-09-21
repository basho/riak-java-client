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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.client.util.UnmodifiableIterator;

/**
 * The default implementation of {@link IRiakObject}
 * <p>
 * Is as immutable as possible. <code>Value</code>, <code>content-type</code>, <code>links</code> and <code>user meta data</code> are all mutable.
 * It is safe to use the instances of this class from multiple threads.
 * </p>
 * <p>
 * Due to the large number of arguments to the constructor the *best* way to create instances is with a {@link RiakObjectBuilder}.
 * </p>
 * 
 * @author russell
 */
public class DefaultRiakObject implements IRiakObject {

    /**
     * The default content type assigned when persisted in Riak if non is provided.
     */
    public static String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    private final String bucket;
    @RiakKey private final String key;
    private final VClock vclock;
    private final String vtag;
    private final long lastModified;

    private final Object linksLock = new Object();
    private final Collection<RiakLink> links;
    private final Object userMetaLock = new Object();
    private final Map<String, String> userMeta;
    private final RiakIndexes indexes;

    private volatile String contentType;
    private volatile byte[] value;

    /**
     * Large number of arguments due to largely immutable nature. Use {@link RiakObjectBuilder} to create instances.
     * 
     * @param bucket the bucket the object is stored in
     * @param key the key it is stored under
     * @param vclock the vclock, if available
     * @param vtag the version tag, if relevant
     * @param lastModified the last modified date from Riak (if relevant)
     * @param contentType the content-type of the value
     * @param value a byte[] of the data payload to store in Riak. Note: this is cloned on construction of this instance.
     * @param links the List of {@link RiakLink}s from this object. Note: this is copied.
     * @param userMeta the {@link Map} of user meta data to store/stored with this object. Note: this is copied.
     * @param indexes the {@link RiakIndexes} for this object. These will be copied to a new {@link RiakIndexes}
     */
    public DefaultRiakObject(String bucket, String key, VClock vclock, String vtag, final Date lastModified,
            String contentType, byte[] value, final Collection<RiakLink> links, final Map<String, String> userMeta,
            final RiakIndexes indexes) {

        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        this.bucket = bucket;
        this.key = key;
        this.vclock = vclock;
        this.vtag = vtag;
        this.lastModified = lastModified == null ? 0 : lastModified.getTime();
        safeSetContentType(contentType);
        this.value = copy(value);
        this.links = copy(links);
        this.userMeta = copy(userMeta);
        this.indexes = RiakIndexes.from(indexes);
    }

    /**
     * Copy the value array.
     * @param value
     * @return a clone of value or null (if value was null)
     */
    private byte[] copy(byte[] value) {
        if (value == null) {
            return null;
        } else {
            return value.clone();
        }
    }

    /**
     * Copy the user meta data
     * @param userMeta
     * @return a copy of user meta data or any empty map
     */
    private Map<String, String> copy(Map<String, String> userMeta) {
        Map<String, String> copy;

        if (userMeta == null) {
            copy = new HashMap<String, String>();
        } else {
            copy = new HashMap<String, String>(userMeta);
        }

        return copy;
    }

    /**
     * Copy the Collection
     * @param <T> the type
     * @param l the collection to copy
     * @return a copy of the collection (or an empty collection if <code>l</code> is null)
     */
    private <T> Collection<T> copy(Collection<T> l) {
        Collection<T> copy;
        if (l == null) {
            copy = new ArrayList<T>();
        } else {
            copy = new ArrayList<T>(l);
        }
        return copy;
    }

    /**
     * If content-type is null set the content-type to DEFAULT_CONTENT_TYPE
     * @param contentType
     */
    private void safeSetContentType(String contentType) {
        if (contentType == null) {
            this.contentType = DEFAULT_CONTENT_TYPE;
        } else {
            this.contentType = contentType;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getBucket()
     */
    public String getBucket() {
        return bucket;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getVClock()
     */
    public VClock getVClock() {
        return vclock;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getKey()
     */
    public String getKey() {
        return key;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getVtag()
     */
    public String getVtag() {
        return vtag;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getLastModified()
     */
    public Date getLastModified() {
        Date lastModified = null;

        if (this.lastModified != 0) {
            lastModified = new Date(this.lastModified);
        }

        return lastModified;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getContentType()
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * NOTE: a copy is returned. Mutating the return value will not effect the state of this instance.
     * @see com.basho.riak.client.IRiakObject#getMeta()
     */
    public Map<String, String> getMeta() {
        return new HashMap<String, String>(userMeta);
    }

    /**
     * @return a *cop* of this object's data payload.
     * @see com.basho.riak.client.IRiakObject#getValue()
     */
    public byte[] getValue() {
        return copy(value);
    }

    // mutate
    /**
     * Note: Copies the value.
     * 
     * @param a byte[] to store in Riak.
     */
    public void setValue(byte[] value) {
        this.value = copy(value);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#setValue(java.lang.String)
     */
    public void setValue(String value) {
        this.value = CharsetUtils.utf8StringToBytes(value);
        this.contentType = CharsetUtils.addUtf8Charset(contentType);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#setContentType(java.lang.String)
     */
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    /**
     * an {@link UnmodifiableIterator} view on the RiakLinks
     */
    public Iterator<RiakLink> iterator() {
        return new UnmodifiableIterator<RiakLink>(getLinks().iterator());
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#addLink(com.basho.riak.client.RiakLink)
     */
    public IRiakObject addLink(RiakLink link) {
        if (link != null) {
            synchronized (linksLock) {
                links.add(link);
            }
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#removeLink(com.basho.riak.client.RiakLink)
     */
    public IRiakObject removeLink(final RiakLink link) {
        synchronized (linksLock) {
            this.links.remove(link);
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#hasLinks()
     */
    public boolean hasLinks() {
        synchronized (linksLock) {
            return !links.isEmpty();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#numLinks()
     */
    public int numLinks() {
        synchronized (linksLock) {
            return links.size();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getLinks()
     */
    public List<RiakLink> getLinks() {
        synchronized (linksLock) {
            return new ArrayList<RiakLink>(links);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#hasLink(com.basho.riak.client.RiakLink)
     */
    public boolean hasLink(final RiakLink riakLink) {
        synchronized (linksLock) {
            return links.contains(riakLink);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#addUsermeta(java.lang.String, java.lang.String)
     */
    public IRiakObject addUsermeta(String key, String value) {
        synchronized (userMetaLock) {
            userMeta.put(key, value);
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#hasUsermeta()
     */
    public boolean hasUsermeta() {
        synchronized (userMetaLock) {
            return !userMeta.isEmpty();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#hasUsermeta(java.lang.String)
     */
    public boolean hasUsermeta(String key) {
        synchronized (userMetaLock) {
            return userMeta.containsKey(key);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getUsermeta(java.lang.String)
     */
    public String getUsermeta(String key) {
        synchronized (userMetaLock) {
            return userMeta.get(key);
        }

    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#removeUsermeta(java.lang.String)
     */
    public IRiakObject removeUsermeta(String key) {
        synchronized (userMetaLock) {
            userMeta.remove(key);
        }
        return this;
    }

    /**
     * return an unmodifiable view of the user meta entries. Attempts to modify
     * will throw UnsupportedOperationException.
     */
    public Iterable<Map.Entry<String, String>> userMetaEntries() {
        return Collections.unmodifiableCollection(userMeta.entrySet());
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.newapi.RiakObject#getVClockAsString()
     */
    public String getVClockAsString() {
        if (vclock != null) {
            return vclock.asString();
        }
        return null;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#getValueAsString()
     */
    public String getValueAsString() {
        return CharsetUtils.asString(value, CharsetUtils.getCharset(contentType));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.IRiakObject#allBinIndexes()
     */
    public Map<BinIndex, Set<String>> allBinIndexes() {
        return indexes.getBinIndexes();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.IRiakObject#getBinIndex(java.lang.String)
     */
    public Set<String> getBinIndex(String name) {
        return indexes.getBinIndex(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.IRiakObject#allIntIndexes()
     */
    public Map<IntIndex, Set<Integer>> allIntIndexes() {
        return indexes.getIntIndexes();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.IRiakObject#getIntIndex(java.lang.String)
     */
    public Set<Integer> getIntIndex(String name) {
        return indexes.getIntIndex(name);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#addIndex(java.lang.String, java.lang.String)
     */
    public IRiakObject addIndex(String index, String value) {
        indexes.add(index, value);
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#addIndex(java.lang.String, int)
     */
    public IRiakObject addIndex(String index, int value) {
        indexes.add(index, value);
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#removeIndex(com.basho.riak.client.query.indexes.BinIndex)
     */
    public IRiakObject removeBinIndex(String index) {
        indexes.removeAll(BinIndex.named(index));
        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakObject#removeIndex(com.basho.riak.client.query.indexes.IntIndex)
     */
    public IRiakObject removeIntIndex(String index) {
        indexes.removeAll(IntIndex.named(index));
        return this;
    }
}
