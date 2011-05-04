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

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.util.UnmodifiableIterator;

/**
 * An implementation of {@link IRiakObject} that also contains the deprecated
 * http.RiakObject methods to facilitate transition between versions.
 *
 * A RiakObject models the meta data and data stored at a bucket/key location in
 * Riak.
 * 
 * @author russell
 */
public class DefaultRiakObject implements IRiakObject {

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

    private volatile String contentType;
    private volatile String value;

    /**
     * Use the builder.
     *
     * @param bucket
     * @param key
     * @param vclock
     * @param conflict
     * @param vtag
     * @param lastModified
     * @param contentType
     * @param value
     * @param siblings
     * @param links
     * @param userMeta
     */
    public DefaultRiakObject(String bucket, String key, VClock vclock, String vtag, final Date lastModified,
            String contentType, String value, final Collection<RiakLink> links, final Map<String, String> userMeta) {

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
        this.value = value;
        this.links = copy(links);
        this.userMeta = copy(userMeta);
    }

    private Map<String, String> copy(Map<String, String> userMeta) {
        Map<String, String> copy;

        if (userMeta == null) {
            copy = new HashMap<String, String>();
        } else {
            copy = new HashMap<String, String>(userMeta);
        }

        return copy;
    }

    private Collection<RiakLink> copy(Collection<RiakLink> links) {
        Collection<RiakLink> copy;
        if (links == null) {
            copy = new ArrayList<RiakLink>();
        } else {
            copy = new ArrayList<RiakLink>(links);
        }
        return copy;
    }

    private void safeSetContentType(String contentType) {
        if (contentType == null) {
            this.contentType = DEFAULT_CONTENT_TYPE;
        } else {
            this.contentType = contentType;
        }
    }

    public String getBucket() {
        return bucket;
    }

    public VClock getVClock() {
        return vclock;
    }

    public String getKey() {
        return key;
    }

    public String getVtag() {
        return vtag;
    }

    public Date getLastModified() {
        Date lastModified = null;

        if (this.lastModified != 0) {
            lastModified = new Date(this.lastModified);
        }

        return lastModified;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, String> getMeta() {
        return new HashMap<String, String>(userMeta);
    }

    public String getValue() {
        return value;
    }

    // mutate

    public void setValue(String value) {
        this.value = value;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    /**
     * an UnmodifiableIterator view on the RiakLinks
     */
    public Iterator<RiakLink> iterator() {
        return new UnmodifiableIterator<RiakLink>(getLinks().iterator());
    }

    
    /**
     * Add link to this RiakObject's links.
     * 
     * @param link
     *            a {@link RiakLink} to add.
     * @return this RiakObject.
     */
    public IRiakObject addLink(RiakLink link) {
        if (link != null) {
            synchronized (linksLock) {
                links.add(link);
            }
        }
        return this;
    }

    /**
     * Remove a {@link RiakLink} from this RiakObject.
     * 
     * @param link
     *            the {@link RiakLink} to remove
     * @return this RiakObject
     */
    public IRiakObject removeLink(final RiakLink link) {
        synchronized (linksLock) {
            this.links.remove(link);
        }
        return this;
    }

    /**
     * Does this RiakObject have any {@link RiakLink}s?
     *
     * @return true if there are links, false otherwise
     */
    public boolean hasLinks() {
        synchronized (linksLock) {
            return !links.isEmpty();
        }
    }

    /**
     * How many {@link RiakLink}s does this RiakObject have?
     *
     * @return the number of {@link RiakLink}s this object has.
     */
    public int numLinks() {
        synchronized (linksLock) {
            return links.size();
        }
    }

    /**
     * Return a copy of the links.
     */
    public List<RiakLink> getLinks() {
        synchronized (linksLock) {
            return new ArrayList<RiakLink>(links);
        }
    }

    /**
     * Checks if the collection of RiakLinks contains the one passed in.
     * 
     * @param riakLink
     *            a RiakLink
     * @return true if the RiakObject's link collection contains riakLink.
     */
    public boolean hasLink(final RiakLink riakLink) {
        synchronized (linksLock) {
            return links.contains(riakLink);
        }
    }

    /**
     * Adds the key, value to the collection of user meta for this object.
     * 
     * @param key
     * @param value
     * @return this RiakObject.
     */
    public IRiakObject addUsermeta(String key, String value) {
        synchronized (userMetaLock) {
            userMeta.put(key, value);
        }
        return this;
    }

    /**
     * @return true if there are any user meta data set on this RiakObject.
     */
    public boolean hasUsermeta() {
        synchronized (userMetaLock) {
            return !userMeta.isEmpty();
        }
    }

    /**
     * @param key
     * @return
     */
    public boolean hasUsermeta(String key) {
        synchronized (userMetaLock) {
            return userMeta.containsKey(key);
        }
    }

    /**
     * Get an item of user meta data.
     *
     * @param key
     *            the user meta data item key
     * @return The value for the given key or null.
     */
    public String getUsermeta(String key) {
        synchronized (userMetaLock) {
            return userMeta.get(key);
        }

    }

    /**
     * @param key
     *            the key of the item to remove
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

}
