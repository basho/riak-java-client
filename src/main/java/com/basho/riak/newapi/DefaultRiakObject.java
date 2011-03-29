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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.VClock;

/**
 * @author russell
 *
 */
public class DefaultRiakObject implements RiakObject {
    private final Bucket bucket;
    private final String key;
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
    public DefaultRiakObject(Bucket bucket, String key, VClock vclock, String vtag, final Date lastModified,
            String contentType, String value, final Collection<RiakLink> links,
            final Map<String, String> userMeta) {

        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        if(key == null) {
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
            this.contentType = "";
        } else {
            this.contentType = contentType;
        }
    }

    private Collection<RiakObject> deepCopy(final Collection<RiakObject> siblings) {
        final ArrayList<RiakObject> copy = new ArrayList<RiakObject>();

        if (siblings != null && siblings.size() == 0) {
            for (RiakObject o : siblings) {
                copy.add(RiakObjectBuilder.from(o).build());
            }
        }

        return copy;
    }

    public Iterator<RiakLink> iterator() {
        return links.iterator();
    }

    public Bucket getBucket() {
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

    public String getBucketName() {
        return bucket.getName();
    }

    public String getValue() {
        return value;
    }

    // mutate

    public RiakObject setValue(String value) {
        this.value = value;
        return this;
    }

    public RiakObject setContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Add link to this RiakObject's links.
     * 
     * @param link
     *            a {@link RiakLink} to add.
     * @return this RiakObject.
     */
    public RiakObject addLink(RiakLink link) {
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
    public RiakObject removeLink(final RiakLink link) {
        synchronized (linksLock) {
            this.links.remove(link);
        }
        return this;
    }

    /**
     * Does this RiakObject has any {@link RiakLink}s?
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

    public Collection<RiakLink> getLinks() {
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
    public RiakObject addUsermeta(String key, String value) {
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
    public RiakObject removeUsermeta(String key) {
        synchronized (userMetaLock) {
            userMeta.remove(key);
        }
        return this;
    }

}
