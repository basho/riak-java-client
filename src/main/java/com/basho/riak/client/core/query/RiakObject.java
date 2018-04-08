/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.core.query;

import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.links.RiakLinks;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.CharsetUtils;

import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * Represents the data and metadata stored in Riak
 * <p>
 * While the client APIs provide methods to store/retrieve data to/from Riak using your own
 * Beans/POJOs, this class is used as the core data type to and from which those types are
 * converted.
 * </p>
 * <p>
 * <h5>Working with Metadata</h5>
 * An object in Riak has a few types of metadata that can be attached; Secondary
 * Indexes, Links, and User Metadata. Each of these has its own container in
 * {@code RiakObject} and methods are provided to access them.
 * </p>
 * <p>
 * <br/><b>Thread Safety:</b><br/>
 * RiakObject is designed to be thread safe. All methods
 * which update the object do so via a thread safe mechanism. The only caveat is
 * that if you use any of the methods prefixed with "unsafe" you need to
 * understand the ramifications as noted in their Javadoc.
 * </p>
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class RiakObject implements Serializable
{
    /**
     * The default content type assigned when storing in Riak if one is not
     * provided. {@value #DEFAULT_CONTENT_TYPE}
     *
     * @see RiakObject#setContentType(java.lang.String)
     */
    public final static String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final long serialVersionUID = 484390882043340231L;

    // Mutable types.
    // Worth noting here is that changes to the contents of this
    // are not guaranteed to be seen outside of a single thread. We never
    // expose it directly outside the RiakObject except via "unsafe" methods
    private volatile BinaryValue value;

    // Mutable collections
    private volatile RiakIndexes riakIndexes;
    private volatile RiakLinks links;
    private volatile RiakUserMetadata userMeta;

    // All immutable types
    private volatile String contentType = DEFAULT_CONTENT_TYPE;
    private volatile String charset;
    private volatile String vtag;
    private volatile boolean isDeleted;
    private volatile boolean isModified;

    // This is annotated to that the UpdateValue command
    // can inject a vclock.
    @RiakVClock
    private volatile VClock vclock;

    private volatile long lastModified;

    /**
     * Constructs a new, empty RiakObject.
     */
    public RiakObject()
    {
    }

    // Methods For dealing with Value

    /**
     * Returns whether or not this RiakObject has a value
     *
     * @return true if the value has been asSet, false otherwise
     */
    public boolean hasValue()
    {
        return value != null;
    }

    /**
     * Returns the value of this RiakObject.
     *
     * @return the value of this RiakObject
     */
    public BinaryValue getValue()
    {
        return value;
    }

    /**
     * Set the value for this RiakObject
     * <p>
     * Note that if {@link RiakObject#setContentType(java.lang.String)} is not
     * called a value of {@value #DEFAULT_CONTENT_TYPE} is used.
     * </p>
     * <br/><b>Thread Safety:</b><br/>
     * Unsafe modifications to the supplied BinaryValue's
     * underlying byte[] would be a bad thing.
     *
     * @param value the value to be stored in Riak.
     * @return a reference to this object.
     * @throws IllegalArgumentException if {@code value} is zero length.
     */
    public RiakObject setValue(BinaryValue value)
    {
        if (value != null && value.length() == 0)
        {
            throw new IllegalArgumentException("value can not be zero length");
        }
        this.value = value;
        return this;
    }

    /**
     * Get the vector clock for this RiakObject.
     *
     * @return The vector clock, or null if not set.
     */
    public VClock getVClock()
    {
        return vclock;
    }

    /**
     * Set the vector clock for this RiakObject.
     *
     * @param vclock a vector clock.
     * @return a reference to this object.
     */
    public RiakObject setVClock(VClock vclock)
    {
        this.vclock = vclock;
        return this;
    }

    /**
     * Returns the version tag (if it is one of a asSet of siblings) for this RiakObject
     *
     * @return the vtag if present, otherwise {@code null}
     */
    public String getVTag()
    {
        return vtag;
    }

    /**
     * Set the version tag for this RiakObject
     *
     * @param vtag a {@code String} representing the VTag for this RiakObject
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code vtag} is zero length
     */
    public RiakObject setVTag(String vtag)
    {
        if (vtag != null && vtag.isEmpty())
        {
            throw new IllegalArgumentException("vtag can not be zero length");
        }
        this.vtag = vtag;
        return this;
    }

    /**
     * Returns the last modified time of this RiakObject.
     * <p>
     * The timestamp is returned as a {@code long} (Unix) epoch time.
     * </p>
     *
     * @return The last modified time or {@code 0} if it has not been asSet.
     * @see RiakObject#setLastModified(long)
     */
    public long getLastModified()
    {
        return lastModified;
    }

    /**
     * A {@code long} timestamp of milliseconds since the epoch to asSet as
     * the last modified date on this RiakObject.
     *
     * @param lastModified
     * @return a reference to this object
     */
    public RiakObject setLastModified(long lastModified)
    {
        this.lastModified = lastModified;
        return this;
    }

    /**
     * Returns the content type of the data payload (value) of this RiakObject
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header.
     * </p>
     *
     * @return a {@code String} representing the content type of this object's value
     * @see RiakObject#setVClock(com.basho.riak.client.api.cap.VClock)
     */
    public String getContentType()
    {
        return contentType;
    }

    /**
     * Set the content type of the data payload (value) of the this RiakObject.
     * Please do not include a charset here, use {@link RiakObject#setCharset(String)} to declare one.
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header.
     * </p>
     *
     * @param contentType a {@code String} representing the content type of this object's value
     * @return a reference to this object
     * @see RiakObject#setValue(com.basho.riak.client.core.util.BinaryValue)
     */
    public RiakObject setContentType(String contentType)
    {
        this.contentType = contentType;
        return this;
    }

    /**
     * Determine if there is a charset set in either the charset or content-type settings.
     *
     * @return true if a charset is present, false otherwise.
     */
    public boolean hasCharset()
    {
        return charset != null || CharsetUtils.hasCharset(this.contentType);
    }

    /**
     * Get the charset for this RiakObject's content (value).
     * Will fetch from the RiakObject's charset setting, and failing that will attempt to get one from the content-type.
     * Defaults to UTF-8.
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header.
     * </p>
     *
     * @return The character asSet {@code String}
     * @see RiakObject#setCharset(java.lang.String)
     * @see RiakObject#setValue(com.basho.riak.client.core.util.BinaryValue)
     */
    public String getCharset()
    {
        if (charset != null)
        {
            return charset;
        }

        return CharsetUtils.getDeclaredCharset(contentType);
    }

    /**
     * Set the characterSet for this object's content (value).
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header.
     * </p>
     *
     * @param charset the {@link Charset} to be asSet
     * @return a reference to this object
     * @see RiakObject#setValue(com.basho.riak.client.core.util.BinaryValue)
     */
    public RiakObject setCharset(String charset)
    {
        this.charset = charset;
        return this;
    }

    // Indexes

    /**
     * Returns whether this RiakObject has secondary indexes (2i)
     *
     * @return {@code true} if indexes are present, {@code false} otherwise
     * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/2i/">Riak Secondary Indexes</a>
     */
    public boolean hasIndexes()
    {
        return (riakIndexes != null && !riakIndexes.isEmpty());
    }

    /**
     * Returns the Secondary Indexes (2i) for this RiakObject
     * <p>
     * Any/all indexes for this {@code RiakObject} are encapsulated in the returned {@code RiakIndexes} object.
     * </p>
     * <br/><b>Thread Safety:</b><br/> The returned <code>RiakIndexes</code> object encapsulates any/all
     * indexes for this <code>RiakObject}</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RiakObject</code>.
     *
     * @return the {@link RiakIndexes} that encapsulates any/all indexes for this RiakObject
     * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/2i/">Riak Secondary Indexes</a>
     */
    public synchronized RiakIndexes getIndexes()
    {
        // Lazy initialization of the internal container.
        if (null == riakIndexes)
        {
            riakIndexes = new RiakIndexes();
        }
        return riakIndexes;
    }

    // Links

    /**
     * Returns whether this RiakObject containsKeyKey links
     *
     * @return {@code true} if links are present, {@code false} otherwise
     */
    public boolean hasLinks()
    {
        return (links != null && !links.isEmpty());
    }

    /**
     * Returns the RiakLinks for this RiakObject.
     * <br/><b>Thread Safety:</b><br/> The returned <code>RiakLinks</code> object encapsulates any/all
     * links for this <code>RiakObject</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RiakObject</code>.
     *
     * @return the {@link RiakIndexes} that encapsulates all/any links for this {@code RiakObject}
     */
    public synchronized RiakLinks getLinks()
    {
        // Lazy initialization of container
        if (null == links)
        {
            links = new RiakLinks();
        }

        return links;
    }

    // User Meta

    /**
     * Returns if there are any User Meta entries for this RiakObject
     *
     * @return {@code true} if user meta entries are present, {@code false} otherwise.
     */
    public boolean hasUserMeta()
    {
        return userMeta != null && !userMeta.isEmpty();
    }

    /**
     * Returns the User Meta for this RiakObject
     * <br/><b>Thread Safety:</b><br/>
     * The returned <code>RiakUserMetadata</code> encapsulates any/all
     * user meta entries for this <code>RiakObject</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RiakObject</code>.
     *
     * @return the {@code RiakUserMetadata} that containsKeyKey any/all User Meta entries for this {@code RiakObject}
     */
    public synchronized RiakUserMetadata getUserMeta()
    {
        // Lazy initialization of container.
        if (null == userMeta)
        {
            userMeta = new RiakUserMetadata();
        }

        return userMeta;
    }

    /**
     * Returns whether or not this RiakObject is marked as being deleted (a tombstone)
     *
     * @return [{@code true} is this {@code RiakObject} is a tombstone, {@code false} otherwise
     */
    public boolean isDeleted()
    {
        return isDeleted;
    }

    /**
     * Marks this RiakObject as being a tombstone in Riak
     *
     * @param isDeleted
     * @return a reference to this object
     */
    public RiakObject setDeleted(boolean isDeleted)
    {
        this.isDeleted = isDeleted;
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        RiakObject that = (RiakObject) o;

        if (isDeleted != that.isDeleted)
        {
            return false;
        }
        if (isModified != that.isModified)
        {
            return false;
        }
        if (lastModified != that.lastModified)
        {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null)
        {
            return false;
        }
        if (riakIndexes != null ? !riakIndexes.equals(that.riakIndexes) : that.riakIndexes != null)
        {
            return false;
        }
        if (links != null ? !links.equals(that.links) : that.links != null)
        {
            return false;
        }
        if (userMeta != null ? !userMeta.equals(that.userMeta) : that.userMeta != null)
        {
            return false;
        }
        if (contentType != null ? !contentType.equals(that.contentType) : that.contentType != null)
        {
            return false;
        }
        if (charset != null ? !charset.equals(that.charset) : that.charset != null)
        {
            return false;
        }
        if (vtag != null ? !vtag.equals(that.vtag) : that.vtag != null)
        {
            return false;
        }
        if (vclock != null ? !vclock.equals(that.vclock) : that.vclock != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (riakIndexes != null ? riakIndexes.hashCode() : 0);
        result = 31 * result + (links != null ? links.hashCode() : 0);
        result = 31 * result + (userMeta != null ? userMeta.hashCode() : 0);
        result = 31 * result + (contentType != null ? contentType.hashCode() : 0);
        result = 31 * result + (charset != null ? charset.hashCode() : 0);
        result = 31 * result + (vtag != null ? vtag.hashCode() : 0);
        result = 31 * result + (isDeleted ? 1 : 0);
        result = 31 * result + (isModified ? 1 : 0);
        result = 31 * result + (vclock != null ? vclock.hashCode() : 0);
        result = 31 * result + (int) (lastModified ^ (lastModified >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "RiakObject{" +
                "contentType: " + contentType +
                ", value: " + value +
                ", riakIndexes: " + riakIndexes +
                ", links: " + links +
                ", userMeta: " + userMeta +
                ", vtag: " + vtag +
                ", isDeleted: " + isDeleted +
                ", isModified: " + isModified +
                ", vclock: " + vclock +
                ", lastModified: " + lastModified +
                '}';
    }
}
