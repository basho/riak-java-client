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

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The default implementation of {@link RiakObject}
 * <p>
 * Is as immutable as possible. 
 * The value, content type, charset,links and user meta data are all mutable.
 * It is safe to use the instances of this class from multiple threads.
 * </p>
 * <p>
 * Due to the large number of arguments, the {@link Builder} is used to create instances.
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public class DefaultRiakObject implements RiakObject
{

    /**
     * The default content type assigned when persisted in Riak if non is
     * provided.
     */
    public final static String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private volatile byte[] key;
    private volatile byte[] bucket;
    private volatile byte[] value;
    private volatile VClock vclock;
    private volatile String vtag;
    private volatile long lastModified;
    private volatile boolean isDeleted;
    private volatile boolean isModified = true;
    private volatile String contentType;
    private volatile Charset charset; // = Charset.forName("ISO-8859-1");
    private final ReentrantReadWriteLock linkLock = new ReentrantReadWriteLock();
    private final Collection<RiakLink> links;
    private final ReentrantReadWriteLock metaLock = new ReentrantReadWriteLock();
    private final Map<String, String> userMeta;
    private final RiakIndexes indexes;

    private DefaultRiakObject(Builder builder)
    {
        if (null == builder.bucket)
        {
            throw new IllegalArgumentException("Bucket can not be null");
        }
        
        this.key = (null == builder.key ? null : Arrays.copyOf(builder.key, builder.key.length));
        this.bucket = Arrays.copyOf(builder.bucket, builder.bucket.length);
        this.value = Arrays.copyOf(builder.value, builder.value.length);
        this.vclock = builder.vclock;
        this.vtag = builder.vtag;
        this.lastModified = (null == builder.lastModified ? 0 : builder.lastModified.getTime());
        this.contentType = (null == builder.contentType ? DEFAULT_CONTENT_TYPE : builder.contentType);
        this.charset = builder.charset;
        this.links = new ArrayList<RiakLink>(builder.links);
        this.userMeta = new HashMap<String,String>(builder.userMeta);
        this.indexes = RiakIndexes.from(builder.indexes);
    }

    @Override
    public String getKeyAsString()
    {
        return new String(key);
    }

    @Override
    public String getKeyAsString(Charset charset)
    {
        return new String(key, charset);
    }

    @Override
    public byte[] getKey()
    {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public String getBucketAsString()
    {
        return new String(bucket);
    }

    @Override
    public String getBucketAsString(Charset charset)
    {
        return new String(bucket, charset);
    }

    @Override
    public byte[] getBucket()
    {
        return Arrays.copyOf(bucket, bucket.length);
    }

    @Override
    public String getValueAsString()
    {
        return new String(value, charset == null ? Charset.defaultCharset() : charset);
    }

    @Override
    public byte[] getValue()
    {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public VClock getVClock()
    {
        return vclock;
    }

    @Override
    public String getVtag()
    {
        return vtag;
    }

    @Override
    public Date getLastModified()
    {
        Date lastModifiedDate = null;

        if (lastModified != 0)
        {
            lastModifiedDate = new Date(this.lastModified);
        }

        return lastModifiedDate;
    }

    @Override
    public String getContentType()
    {
        return contentType;
    }

    @Override
    public Charset getCharset()
    {
        return charset;
    }

    @Override
    public void setValue(byte[] value)
    {
        this.value = Arrays.copyOf(value, value.length);
    }

    @Override
    public void setValue(String value)
    {
        this.value = value.getBytes();
        this.charset = Charset.defaultCharset();
    }

    @Override
    public void setValue(String value, Charset charset)
    {
        this.value = value.getBytes();
        this.charset = charset;
    }

    @Override
    public void setContentType(String contentType)
    {
        if (contentType != null)
        {
            this.contentType = contentType;
        }
        else
        {
            this.contentType = DEFAULT_CONTENT_TYPE;
        }
    }

    @Override
    public void setCharset(Charset charset)
    {
        this.charset = charset;
    }

    @Override
    public String getVClockAsString()
    {
        if (vclock != null)
        {
            return vclock.asString();
        }
        return null;
    }

    @Override
    public List<RiakLink> getLinks()
    {
        try
        {
            linkLock.readLock().lock();
            return new ArrayList<RiakLink>(links);
        }
        finally
        {
            linkLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasLinks()
    {
        try
        {
            linkLock.readLock().lock();
            return !links.isEmpty();
        }
        finally
        {
            linkLock.readLock().lock();
        }
    }

    @Override
    public int numLinks()
    {
        try
        {
            linkLock.readLock().lock();
            return links.size();
        }
        finally
        {
            linkLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasLink(RiakLink riakLink)
    {
        try
        {
            linkLock.readLock().lock();
            return links.contains(riakLink);
        }
        finally
        {
            linkLock.readLock().unlock();
        }
    }

    @Override
    public RiakObject removeLink(RiakLink link)
    {
        try
        {
            linkLock.writeLock().lock();
            links.remove(link);
        }
        finally
        {
            linkLock.writeLock().unlock();
        }
        return this;
    }

    @Override
    public RiakObject addLink(RiakLink link)
    {
        if (link != null)
        {
            try
            {
                linkLock.writeLock().lock();
                links.add(link);
            }
            finally
            {
                linkLock.writeLock().unlock();
            }
        }
        return this;
    }

    @Override
    public Map<String, String> getMeta()
    {
        try
        {
            metaLock.readLock().lock();
            return new HashMap<String, String>(userMeta);
        }
        finally
        {
            metaLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasUsermeta()
    {
        try
        {
            metaLock.readLock().lock();
            return !userMeta.isEmpty();
        }
        finally
        {
            metaLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasUsermeta(String key)
    {
        try
        {
            metaLock.readLock().lock();
            return userMeta.containsKey(key);
        }
        finally
        {
            metaLock.readLock().unlock();
        }
    }

    @Override
    public String getUsermeta(String key)
    {
        try
        {
            metaLock.readLock().lock();
            return userMeta.get(key);
        }
        finally
        {
            metaLock.readLock().unlock();
        }
    }

    @Override
    public Iterable<Entry<String, String>> userMetaEntries()
    {
        try
        {
            metaLock.readLock().lock();
            return Collections.unmodifiableCollection(userMeta.entrySet());
        }
        finally
        {
            metaLock.readLock().unlock();
        }
    }

    @Override
    public RiakObject addUsermeta(String key, String value)
    {
        try
        {
            metaLock.writeLock().lock();
            userMeta.put(key, value);
        }
        finally
        {
            metaLock.writeLock().unlock();
        }
        return this;
    }

    @Override
    public RiakObject removeUsermeta(String key)
    {
        try
        {
            metaLock.writeLock().lock();
            userMeta.remove(key);
        }
        finally
        {
            metaLock.writeLock().unlock();
        }
        return this;
    }

    @Override
    public Map<BinIndex, Set<String>> allBinIndexes()
    {
        return indexes.getBinIndexes();
    }

    @Override
    public Set<String> getBinIndex(String name)
    {
        return indexes.getBinIndex(name);
    }

    @Override
    public Map<IntIndex, Set<Long>> allIntIndexes()
    {
        return indexes.getIntIndexes();
    }

    @Override
    public Set<Long> getIntIndex(String name)
    {
        return indexes.getIntIndex(name);
    }

    @Override
    public RiakObject addIndex(String index, String value)
    {
        indexes.add(index, value);
        return this;
    }

    @Override
    public RiakObject addIndex(String index, long value)
    {
        indexes.add(index, value);
        return this;
    }

    @Override
    public RiakObject removeBinIndex(String index)
    {
        indexes.removeAll(BinIndex.named(index));
        return this;
    }

    @Override
    public RiakObject removeIntIndex(String index)
    {
        indexes.removeAll(IntIndex.named(index));
        return this;
    }

    @Override
    public RiakObject removeFromBinIndex(String indexName, String value)
    {
        indexes.remove(indexName, value);
        return this;
    }
    
    @Override
    public RiakObject removeFromIntIndex(String indexName, long value)
    {
        indexes.remove(indexName, value);
        return this;
    }
    
    @Override
    public boolean isDeleted()
    {
        return isDeleted;
    }
    
    @Override
    public boolean isModified()
    {
        return isModified;
    }

    /**
     * Creates instances of {@link DefaultRiakObject}
     *
     * @author Brian Roach <roach at basho dot com>
     * @author Russel Brown <russelldb at basho dot com>
     * @since 2.0
     */
    public static class Builder
    {

        private byte[] key;
        private byte[] bucket;
        private byte[] value;
        private VClock vclock;
        private String vtag;
        private Date lastModified;
        private boolean isDeleted;
        private boolean isModified = true;
        private String contentType = DEFAULT_CONTENT_TYPE;
        private Charset charset; // = Charset.forName("ISO-8859-1");
        private Collection<RiakLink> links = new ArrayList<RiakLink>();
        private Map<String, String> userMeta = new HashMap<String, String>();
        private RiakIndexes indexes = new RiakIndexes();

        /**
         * Creates a new builder for {@link RiakObject} objects
         */
        public Builder()
        {
        }

        /**
         * Creates a builder pre-populated from the given {@link RiakObject}.
         *
         * @param o the {@link RiakObject} to copy
         * @return a {@link Builder} with all fields set from <code>o</code>
         */
        public static Builder from(RiakObject o)
        {
            Builder rob = new Builder();
            rob.key = o.getKey();
            rob.bucket = o.getBucket();
            rob.vclock = o.getVClock();
            rob.contentType = o.getContentType();
            rob.charset = o.getCharset();
            rob.lastModified = o.getLastModified();
            rob.value = o.getValue();
            rob.links = o.getLinks();
            rob.indexes = new RiakIndexes(o.allBinIndexes(), o.allIntIndexes());
            rob.userMeta = o.getMeta();
            rob.isDeleted = o.isDeleted();
            return rob;
        }

        public RiakObject build()
        {
            return new DefaultRiakObject(this);
        }
        
        /**
         * Set the key.
         *
         * The key is stored internally as a byte array. The default encoding
         * will be used to convert the provided string.
         *
         * @param key
         * @return this
         */
        public Builder withKey(String key)
        {
            this.key = key.getBytes();
            return this;
        }

        /**
         * Set the key.
         *
         * The key is stored internally as a byte array. The provided
         * {@code Charset} will be used to convert the provided string.
         *
         * @param key
         * @param charset - the {@code Charset} to use for encoding to a byte
         * array
         * @return this
         */
        public Builder withKey(String key, Charset charset)
        {
            this.key = key.getBytes(charset);
            return this;
        }

        /**
         * Set the key
         *
         * @param key
         * @return this
         */
        public Builder withKey(byte[] key)
        {
            this.key = key;
            return this;
        }

        /**
         * Set the bucket name.
         *
         * The bucket is stored internally as a byte array. The default encoding
         * will be used to convert the provided string.
         *
         * @param bucket - the name of the bucket
         * @return this
         */
        public Builder withBucket(String bucket)
        {
            this.bucket = bucket.getBytes();
            return this;
        }

        /**
         * Set the bucket name.
         *
         * The bucket is stored internally as a byte array. The provided
         * {@code Charset} will be used to convert the provided string.
         *
         * @param bucket - the name of the bucket
         * @param charset - the {@code Charset} to use for encoding to a byte
         * array
         * @return this
         */
        public Builder withBucket(String bucket, Charset charset)
        {
            this.bucket = bucket.getBytes(charset);
            return this;
        }

        /**
         * Set the bucket name
         *
         * @param bucket
         * @return this
         */
        public Builder withBucket(byte[] bucket)
        {
            this.bucket = bucket;
            return this;
        }

        /**
         * The value to give the constructed riak object
         *
         * @param value byte[] or null.
         * @return this
         */
        public Builder withValue(byte[] value)
        {
            this.value = value;
            return this;
        }

        /**
         * Convenience method uses default {@code Charset} for converting the
         * provided {@code String} to a byte array.
         *
         * @param value a UTF-8 encoded string
         * @return this
         */
        public Builder withValue(String value)
        {
            this.value = value.getBytes();
            this.charset = Charset.defaultCharset();
            return this;
        }

        /**
         * Convenience method uses provided {@code Charset} for converting the
         * provided {@code String} to a byte array.
         *
         * @param value
         * @param charset - {@code Charset} used to convert value
         * @return this
         */
        public Builder withValue(String value, Charset charset)
        {
            this.value = value.getBytes(charset);
            this.charset = charset;
            return this;
        }

        /**
         * The vector clock value for the constructed riak object
         *
         * @param vclock the byte[] of a vector clock: NOTE: will be cloned.
         * @return this
         * @throws IllegalArgumentException for a null vclock
         */
        public Builder withVClock(byte[] vclock)
        {
            this.vclock = new BasicVClock(vclock);
            return this;
        }

        /**
         * The version tag for this riak object
         *
         * @param vtag
         * @return this
         */
        public Builder withVtag(String vtag)
        {
            this.vtag = vtag;
            return this;
        }

        /**
         * A
         * <code>long</code> timestamp of milliseconds since the epoch to set as
         * the last modified date on this Riak object
         *
         * @param lastModified
         * @return this
         */
        public Builder withLastModified(long lastModified)
        {
            this.lastModified = new Date(lastModified);
            return this;
        }

        /**
         * A Collection of {@link RiakLink}s for the new riak object
         *
         * @param links the {@link Collection} of {@link RiakLink}s for the Riak
         * object, is copied over the current collection, not merged! NOTE: this
         * will be copied.
         * @return this
         */
        public Builder withLinks(Collection<RiakLink> links)
        {
            if (links != null)
            {
                this.links = links;
            }
            return this;
        }

        /**
         * Add a {@link RiakLink} to the new riak object's collection.
         *
         * @param bucket the bucket at the end of the link
         * @param key the key at the end of the link
         * @param tag the link tag
         * @return this
         */
        public Builder addLink(String bucket, String key, String tag)
        {
            synchronized (links)
            {
                links.add(new RiakLink(bucket, key, tag));
            }
            return this;
        }

        /**
         * A Collection of {@link com.basho.riak.client.query.indexes.RiakIndex}es for the new riak object
         *
         * @param indexes the {@link Collection} of {@link com.basho.riak.client.query.indexes.RiakIndex}es for the
         * Riak object, is copied over the current collection, not merged! 
         * @return this
         */
        public Builder withIndexes(RiakIndexes indexes)
        {
            this.indexes = RiakIndexes.from(indexes);
            return this;
        }

        /**
         * Add a {@link com.basho.riak.client.query.indexes.RiakIndex} to the new riak object's collection.
         *
         * @param index the name of the index
         * @param value the index value
         * @return this
         */
        public Builder addIndex(String index, long value)
        {
            this.indexes.add(index, value);
            return this;
        }

        /**
         * Add a {@link com.basho.riak.client.query.indexes.RiakIndex} to the new riak object's collection.
         *
         * @param index the name of the index
         * @param value the index value
         * @return this
         */
        public Builder addIndex(String index, String value)
        {
            this.indexes.add(index, value);
            return this;
        }

        /**
         * A map of user meta data to set on the new riak object.
         *
         * @param usermeta a {@link Map}, copied over the current user meta (not
         * merged)
         * @return this
         */
        public Builder withUsermeta(Map<String, String> usermeta)
        {
            if (usermeta != null)
            {
                this.userMeta = usermeta;
            }
            return this;
        }

        /**
         * Add an item of user meta data to the collection for the new Riak
         * object.
         *
         * @param key
         * @param value
         * @return this
         */
        public Builder addUsermeta(String key, String value)
        {
            synchronized (userMeta)
            {
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
        public Builder withContentType(String contentType)
        {
            this.contentType = contentType;
            return this;
        }

        /**
         * The new Riak objects vector clock.
         *
         * @param vclock
         * @return this
         */
        public Builder withVClock(VClock vclock)
        {
            this.vclock = vclock;
            return this;
        }

        /**
         * Marks this object as being a tombstone in Riak
         * 
         * @param isDeleted
         * @return this
         */
        public Builder withDeleted(boolean isDeleted)
        {
            this.isDeleted = isDeleted;
            return this;
        }
        
        /**
         * Marks  this object as being not modified in Riak
         * 
         * @see FetchMeta.Builder#modifiedSince(java.util.Date) 
         * @param isModified
         * @return this
         */
        public Builder withModified(boolean isModified)
        {
            this.isModified = isModified;
            return this;
        }
    }
}
