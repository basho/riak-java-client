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
package com.basho.riak.client.builders;

import static com.basho.riak.client.util.CharsetUtils.utf8StringToBytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.basho.riak.client.DefaultRiakObject;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.util.CharsetUtils;

/**
 * Creates instances of {@link DefaultRiakObject}
 * @author russell
 */
public class RiakObjectBuilder {
    private final String bucket;
    private final String key;
    private byte[] value;
    private VClock vclock;
    private String vtag;
    private Date lastModified;
    private Collection<RiakLink> links = new ArrayList<RiakLink>();
    private RiakIndexes indexes = new RiakIndexes();
    private Map<String, String> userMeta = new HashMap<String, String>();
    private String contentType;
    private boolean isDeleted = false;
    /**
     * Create a new builder for a {@link IRiakObject} at bucket/key
     * 
     * @param bucket
     * @param key
     */
    private RiakObjectBuilder(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * Static factory method
     * 
     * @param bucket
     * @param key
     * @return a new {@link RiakObjectBuilder}
     */
    public static RiakObjectBuilder newBuilder(String bucket, String key) {
        return new RiakObjectBuilder(bucket, key);
    }

    /**
     * Creates a builder prepopulated from the give {@link IRiakObject}.
     * 
     * @param o
     *            the {@link IRiakObject} to copy
     * @return a {@link RiakObjectBuilder} with all fields set from
     *         <code>o</code>
     */
    public static RiakObjectBuilder from(IRiakObject o) {
        RiakObjectBuilder rob = new RiakObjectBuilder(o.getBucket(), o.getKey());
        rob.vclock = o.getVClock();
        rob.contentType = o.getContentType();
        rob.lastModified = o.getLastModified();
        rob.value = o.getValue();
        rob.links = o.getLinks();
        rob.indexes = new RiakIndexes(o.allBinIndexes(), o.allIntIndexesV2());
        rob.userMeta = o.getMeta();
        rob.isDeleted = o.isDeleted();
        return rob;
    }

    /**
     * Construct a {@link DefaultRiakObject} from this builders parameters
     * @return an {@link IRiakObject} with the values from this builder.
     */
    public IRiakObject build() {
        return new DefaultRiakObject(bucket, key, vclock, vtag, lastModified, contentType, value, links, userMeta,
                                     indexes, isDeleted);
    }

    /**
     * The value to give the constructed riak object
     * 
     * @param value
     *            byte[] or null. NOTE: will be cloned.
     * @return this
     */
    public RiakObjectBuilder withValue(byte[] value) {
        this.value =  value==null? null : value.clone();
        return this;
    }

    /**
     * Convenience method assumes a UTF-8 encoded string
     * 
     * @param value
     *            a UTF-8 encoded string
     * @return this
     */
    public RiakObjectBuilder withValue(String value) {
        this.value = utf8StringToBytes(value);
        this.contentType = CharsetUtils.addUtf8Charset(contentType);
        return this;
    }

    /**
     * The vector clock value for the constructed riak object
     * 
     * @param vclock
     *            the byte[] of a vector clock: NOTE: will be cloned.
     * @return this
     * @throws IllegalArgumentException
     *             for a null vclock
     */
    public RiakObjectBuilder withVClock(byte[] vclock) {
        this.vclock = new BasicVClock(vclock);
        return this;
    }

    /**
     * The version tag for this riak object
     * 
     * @param vtag
     * @return this
     */
    public RiakObjectBuilder withVtag(String vtag) {
        this.vtag = vtag;
        return this;
    }

    /**
     * A <code>long</code> timestamp of milliseconds since the epoch to set as
     * the last modified date on this Riak object
     * 
     * @param lastModified
     * @return
     */
    public RiakObjectBuilder withLastModified(long lastModified) {
        this.lastModified = new Date(lastModified);
        return this;
    }

    /**
     * A Collection of {@link RiakLink}s for the new riak object
     * 
     * @param links
     *            the {@link Collection} of {@link RiakLink}s for the Riak
     *            object, is copied over the current collection, not merged!
     *            NOTE: this will be copied.
     * @return this
     */
    public RiakObjectBuilder withLinks(Collection<RiakLink> links) {
        if (links != null) {
            this.links = new ArrayList<RiakLink>(links);
        }
        return this;
    }

    /**
     * Add a {@link RiakLink} to the new riak object's collection.
     * 
     * @param bucket
     *            the bucket at the end of the link
     * @param key
     *            the key at the end of the link
     * @param tag
     *            the link tag
     * @return this
     */
    public RiakObjectBuilder addLink(String bucket, String key, String tag) {
        synchronized (links) {
            links.add(new RiakLink(bucket, key, tag));
        }
        return this;
    }

    /**
     * A Collection of {@link RiakIndex}es for the new riak object
     * 
     * @param indexes
     *            the {@link Collection} of {@link RiakIndex}es for the Riak
     *            object, is copied over the current collection, not merged!
     *            NOTE: this will be copied.
     * @return this
     */
    public RiakObjectBuilder withIndexes(RiakIndexes indexes) {
        this.indexes = RiakIndexes.from(indexes);
        return this;
    }

    /**
     * Add a {@link RiakIndex} to the new riak object's collection.
     * 
     * @param index
     *            the {@link RiakIndex} to add
     * @return this
     */
    public RiakObjectBuilder addIndex(String index, long value) {
        this.indexes.add(index, value);
        return this;
    }

    /**
     * Add a {@link RiakIndex} to the new riak object's collection.
     * 
     * @param index
     *            the {@link RiakIndex} to add
     * @return this
     */
    public RiakObjectBuilder addIndex(String index, String value) {
        this.indexes.add(index, value);
        return this;
    }

    /**
     * A map of user meta data to set on the new riak object.
     * 
     * @param usermeta
     *            a {@link Map}, copied over the current user meta (not merged):
     *            NOTE: is copied.
     * @return this
     */
    public RiakObjectBuilder withUsermeta(Map<String, String> usermeta) {
        if(usermeta != null) {
            this.userMeta = new HashMap<String, String>(usermeta);
        }
        return this;
    }

    /**
     * Add an item of user meta data to the collection for the new Riak object.
     * 
     * @param key
     * @param value
     * @return this
     */
    public RiakObjectBuilder addUsermeta(String key, String value) {
        synchronized (userMeta) {
            userMeta.put(key, value);
        }
        return this;
    }

    /**
     * The content-type of the data payload of the new Riak object.
     * 
     * @param contentType
     * @return this
     */
    public RiakObjectBuilder withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * The new Riak objects vector clock.
     * @param vclock
     * @return this
     */
    public RiakObjectBuilder withVClock(VClock vclock) {
        this.vclock = vclock;
        return this;
    }
    
    public RiakObjectBuilder withDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
        return this;
    }

}
