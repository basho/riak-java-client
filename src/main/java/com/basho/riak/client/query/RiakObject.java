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
package com.basho.riak.client.query;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.CharsetUtils;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Represents the data and metadata stored in Riak
 * <p>
 * While the client APIs provide methods to store/retrieve data to/from Riak using your own
 * Beans/POJOs, this class is used as the core data type to and from which those types are
 * converted. 
 * </p>
 * <p>
 * Static factory methods are provided to create a new {@code RiakObject}. A 
 * fluent interface/API is provided to allow filling out the newly constructed object; 
 * all methods which mutate the object return a reference to the object:
 * </p>
 * <blockquote><pre>
 * 
 * RiakObject myObject = RiakObject.create("my_bucket")
 *                                  .setKey("my_key")
 *                                  .setValue("my_value");
 * </pre></blockquote>
 * <p>
 * <h5>Working with Metadata</h5>
 * An object in Riak has a few types of metadata that can be attached; Secondary
 * Indexes, Links, and User Metadata. Each of these has its own container in
 * {@code RiakObject} and methods are provided to access them. 
 * </p>
 * <p>
 * @riak.threadsafety RiakObject is designed to be thread safe. All methods 
 * which mutate the object do so via a thread safe mechanism. The only caveat is
 * that if you use any of the methods prefixed with "unsafe" you need to
 * understand the ramifications as noted in their Javadoc.
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class RiakObject
{
    /**
     * The default content type assigned when storing in Riak if one is not
     * provided. {@value #DEFAULT_CONTENT_TYPE}
     * @see RiakObject#setContentType(java.lang.String) 
     */
    public final static String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    
    // Mutable types. 
    // Worth noting here is that changes to the contents of these arrays 
    // are not guaranteed to be seen outside of a single thread. We never 
    // expose them directly outside the RiakObject except via "unsafe" methods
    private volatile byte[] key;
    private volatile byte[] bucket;
    private volatile byte[] value;
    private volatile byte[] bucketType;
    
    // Mutable collections 
    private volatile RiakIndexes riakIndexes;
    private volatile RiakLinks links;
    private volatile RiakUserMetadata userMeta;
    
    // All immutable types
    private volatile String contentType;
    private volatile VClock vclock;
    private volatile String vtag;
    private volatile boolean isDeleted;
    private volatile boolean isModified;
    private volatile boolean isNotFound;
    
    private volatile long lastModified;
    
    
    /**
     * Static factory method for creating a new RiakObject
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. This method 
     * will convert the supplied {@code String} using the default {@code Charset}.
     * </p>
     * @param bucketName the name of the bucket as a {@code String}
     * @return a new {@code RiakObject}
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public static RiakObject create(String bucketName)
    {
        return new RiakObject(bucketName.getBytes(Charset.defaultCharset()));
    }
    
    /**
     * Static factory method for creating a new RiakObject
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. This method 
     * will convert the supplied {@code String} using the provided {@code Charset}.
     * </p>
     * @param bucketName the name of the bucket as a {@code String}
     * @param charset the {@link Charset} for this String
     * @return a new {@code RiakObject}
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public static RiakObject create(String bucketName, Charset charset)
    {
        return new RiakObject(bucketName.getBytes(charset));
    }
    
    /**
     * Static factory method for creating a new RiakObject
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param bucketName the {@code byte[]} to be copied and used as the bucket name
     * @return a new {@code RiakObject}
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public static RiakObject create(byte[] bucketName)
    {
        return new RiakObject(Arrays.copyOf(bucketName, bucketName.length));
    }
    
    /**
     * Static factory method for creating a new RiakObject
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is not copied and the reference is used
     * directly. Retaining a reference to this array and making subsequent
     * changes will lead to undefined behavior in regard to thread safety and
     * visibility.
     * @param bucketName the byte array to be used as the bucket name
     * @return a new {@code RiakObject}
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public static RiakObject unsafeCreate(byte[] bucketName)
    {
        return new RiakObject(bucketName);
    }
    
    /**
     * Private constructor  
     * <p>
     * Instances of RiakObject are created via the public static {@code create()} methods.
     * </p>
     * @param bucketName the bucket name as a {@code byte[]}
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    private RiakObject(byte[] bucketName)
    {
        if (null == bucketName || bucketName.length == 0)
        {
            throw new IllegalArgumentException("Bucket name can not be null or zero length");
        }
        this.bucket = bucketName;
    }
    
    // Key
    
    /**
     * Returns the key for this RiakObject as a {@link ByteArrayInputStream}
     * @return a {@code ByteArrayInputStream} backed by the internal {@code byte[]} 
     * or {@code null} if the key has not been set.
     */
    public ByteArrayInputStream getKey()
    {
        if (key != null)
        {
            return new ByteArrayInputStream(key);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Returns the key for this RiakObject as raw bytes.
     * @riak.threadsafety A copy of the internal <code>byte[]</code> is returned.
     * @return a copy of the internal {@code byte[]}
     * or {@code null} if the key has not been set. 
     */
    public byte[] getKeyAsBytes()
    {
        if (key != null)
        {
            return Arrays.copyOf(key, key.length);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Returns the key for this RiakObject as raw bytes.
     * 
     * @riak.threadsafety This method exposes the internal <code>byte[]</code> directly.
     * Modifying the contents of this array will lead to undefined behavior in 
     * regard to thread safety and visibility.
     * @return the internal {@code byte[]} or {@code null} if the key has not been set.
     */
    public byte[] unsafeGetKeyAsBytes()
    {
        return key;
    }
    
    /**
     * Returns the key for this RiakObject String. 
     * <p>
     * The key is stored internally as a {@code byte[]}. This method converts those bytes to
     * a {@code String} using your default {@code Charset}
     * </p>
     * @return The key as {@code String} or {@code null} if the key has not been set. 
     */
    public String getKeyAsString()
    {
        if (key != null)
        {
            return new String(key, Charset.defaultCharset());
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the key for this RiakObject as a String. 
     * <p>
     * The key is stored internally as a {@code byte[]}. 
     * This method returns those bytes encoded as a
     * {@code String} using the supplied {@code Charset}.
     * </p>
     * @param charset the {@link Charset} to use
     * @return The key converted to a {@code String} using the supplied {@code Charset}
     */
    public String getKeyAsString(Charset charset) 
    {
        if (key != null) 
        {
            return new String(key, charset);
        }
        else 
        {
            return null;
        }
    }
    
    /**
     * Set the key for this RiakObject.
     * <p>
     * Riak is character set agnostic. The key is sent as 
     * bytes and stored as bytes. This method 
     * will convert the supplied String using the default {@code Charset}
     * </p>
     * @param key the key as a {@code String}
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code key} is zero length
     */
    public RiakObject setKey(String key) 
    {
        if (null == key)
        {
            this.key = null;
        }
        else if (key.isEmpty())
        {
            throw new IllegalArgumentException("Key can not be zero length");
        }
        else
        {
            this.key = key.getBytes(Charset.defaultCharset());
        }
        
        return this;
    }

    /**
     * Set the key for this RiakObject
     * <p>
     * Riak is character set agnostic. The key is sent as 
     * bytes and stored as bytes. This method 
     * will convert the provided String using the provided {@code Charset}.
     * </p>
     * @param key the key as a {@code String}
     * @param charset the {@link Charset} to use 
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code key} is zero length
     */
    public RiakObject setKey(String key, Charset charset) 
    {
        if (null == key)
        {
            this.key = null;
        }
        else if (key.isEmpty())
        {
            throw new IllegalArgumentException("Key can not be zero length");
        }
        else
        {
            this.key = key.getBytes(charset);
        }
        
        return this;
    }
        
    /**
     * Set the key for this RiakObject.
     * <p>
     * Riak is character set agnostic. The key is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param key a {@code byte[]} to be copied and used as the key
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code key} is zero length
     */
    public RiakObject setKey(byte[] key)
    {
        if (key !=null && key.length == 0)
        {
            throw new IllegalArgumentException("Key can not be zero length");
        }
        this.key = Arrays.copyOf(key, key.length);
        return this;
    }
    
    /**
     * Set the key for this RiakObject.
     * <p>
     * Riak is character set agnostic. The key is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is not copied and the reference is used
     * directly. Retaining a reference to this array and making subsequent
     * changes will lead to undefined behavior in regard to thread safety and
     * visibility. 
     * @param key a {@code byte[]} to be used as the key
     * @return a reference to this object
     * @throws IllegalArgumentException if the {@code key} is zero length
     */
    public RiakObject unsafeSetKey(byte[] key)
    {
        if (key !=null && key.length == 0)
        {
            throw new IllegalArgumentException("Key can not be zero length");
        }
        this.key = key;
        return this;
    }
    
    // Bucket
    
    /**
     * Returns the bucket name for this RiakObject as a {@link ByteArrayInputStream}
     * @return a {@code ByteArrayInputStream} backed by the internal {@code byte[]} 
     */
    public ByteArrayInputStream getBucket()
    {
        return new ByteArrayInputStream(bucket);
    }

    /**
     * Returns the bucket name for this RiakObject as raw bytes.
     * @riak.threadsafety A copy of the internal <code>byte[]</code> is returned.
     * @return a copy of the internal {@code byte[]}
     */
    public byte[] getBucketAsBytes()
    {
        return Arrays.copyOf(bucket, bucket.length);
    }
    
    /**
     * Returns the bucket name for this RiakObject as raw bytes.
     * @riak.threadsafety This method exposes the internal {<code>byte[]</code> directly.
     * Modifying the contents of this array will lead to undefined behavior in 
     * regard to thread safety and visibility.
     * @return the internal {@code byte[]} 
     */
    public byte[] unsafeGetBucketAsBytes()
    {
        return bucket;
    }
    
    /**
     * Returns the bucket name for this RiakObject as a String. 
     * <p>
     * The bucket is stored internally as a {@code byte[]}. This method converts those bytes to
     * a {@code String} using your default {@code Charset}
     * </p>
     * @return The bucket as a {@code String}. 
     */
    public String getBucketAsString()
    {
        return new String(bucket, Charset.defaultCharset());
    }
    
    /**
     * Returns the bucket name for this RiakObject as a String. 
     * <p>
     * The bucket is stored internally as a {@code byte[]}. 
     * This method returns those bytes encoded as a
     * {@code String} using the supplied {@code Charset}.
     * </p>
     * @param charset the {@link Charset} to use
     * @return The bucket name converted to a {@code String} using the supplied {@code Charset}
     */
    public String getBucketAsString(Charset charset) 
    {
        return new String(bucket, charset);
    }
    
    /**
     * Set the bucket name for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket is stored as bytes. This method 
     * will convert the supplied {@code String} using the default {@code Charset}.
     * </p>
     * @param bucketName the bucket name as a {@code String}
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public RiakObject setBucket(String bucketName) 
    {
        if (null == bucketName || bucketName.isEmpty())
        {
            throw new IllegalArgumentException("Bucket name can not be null or zero length");
        }
        this.bucket = bucketName.getBytes(Charset.defaultCharset());
        return this;
    }

    /**
     * Set the bucket name for this RiakObject
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. This method 
     * will convert the provided {@code String} using the provided {@code Charset}.
     * </p>
     * @param bucketName the bucket name as a {@code String}
     * @param charset the {@link Charset} to use 
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public RiakObject setBucket(String bucketName, Charset charset) 
    {
        if (null == bucketName || bucketName.isEmpty())
        {
            throw new IllegalArgumentException("Bucket name can not be null or zero length");
        }
        this.bucket = bucketName.getBytes(charset);
        return this;
    }
        
    /**
     * Set the bucket name for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param bucket a {@code byte[]} to be copied and used as the bucket name
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public RiakObject setBucket(byte[] bucket)
    {
        if (null == bucket || bucket.length == 0)
        {
            throw new IllegalArgumentException("bucket can not be zero length or null");
        }
        this.bucket = Arrays.copyOf(bucket, bucket.length);
        return this;
    }
    
    /**
     * Set the bucket name for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket name is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is not copied and the reference is used
     * directly. Retaining a reference to this array and making subsequent
     * changes will lead to undefined behavior in regard to thread safety and
     * visibility. 
     * @param bucket a {@code byte[]} to be used as the bucket name
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketName} is {@code null} or zero length
     */
    public RiakObject unsafeSetBucket(byte[] bucket)
    {
        if (null == bucket || bucket.length == 0)
        {
            throw new IllegalArgumentException("bucket can not be zero length or null");
        }
        this.bucket = bucket;
        return this;
    }
    
    // Bucket type
    
    /**
     * Returns the bucket type for this RiakObject as a {@link ByteArrayInputStream}
     * @return a {@code ByteArrayInputStream} backed by the internal {@code byte[]} 
     * or {@code null} if the bucket type has not been set.
     */
    public ByteArrayInputStream getBucketType()
    {
        if (bucketType != null)
        {
            return new ByteArrayInputStream(bucketType);
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the bucket type for this RiakObject as raw bytes.
     * @riak.threadsafety A copy of the internal <code>byte[]</code> is returned.
     * @return a copy of the internal {@code byte[]} or {@code null} if the bucket type has not been set.
     */
    public byte[] getBucketTypeAsBytes()
    {
        if (bucketType != null)
        {
            return Arrays.copyOf(bucketType, bucketType.length);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Returns the bucket type for this RiakObject as raw bytes.
     * @riak.threadsafety This method exposes the internal {<code>byte[]</code> directly.
     * Modifying the contents of this array will lead to undefined behavior in 
     * regard to thread safety and visibility.
     * @return the internal {@code byte[]} or {@code null} if the bucket type has not been set.
     */
    public byte[] unsafeGetBucketTypeAsBytes()
    {
        return bucketType;
    }
    
    /**
     * Returns the bucket type for this RiakObject as a String. 
     * <p>
     * The bucket type is stored internally as a {@code byte[]}. This method converts those bytes to
     * a {@code String} using your default {@code Charset}
     * </p>
     * @return The bucket type as a {@code String} or {@code null} if the bucket type has not been set.. 
     */
    public String getBucketTypeAsString()
    {
        if (bucketType != null) 
        {
            return new String(bucketType, Charset.defaultCharset());
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Returns the bucket type for this RiakObject as a String. 
     * <p>
     * The bucket type is stored internally as a {@code byte[]}. 
     * This method returns those bytes encoded as a
     * {@code String} using the supplied {@code Charset}.
     * </p>
     * @param charset the {@link Charset} to use
     * @return The bucket type converted to a {@code String} using the supplied {@code Charset}
     * or {@code null} if the bucket type has not been set.
     */
    public String getBucketTypeAsString(Charset charset) 
    {
        if (bucketType != null)
        {
            return new String(bucketType, charset);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Set the bucket type for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket type is stored as bytes. This method 
     * will convert the supplied {@code String} using the default {@code Charset}.
     * </p>
     * @param bucketType the bucket type as a {@code String}
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketType} is {@code null} or zero length
     */
    public RiakObject setBucketType(String bucketType) 
    {
        if (bucketType != null)
        {
            if (bucketType.isEmpty())
            {
                throw new IllegalArgumentException("Bucket type can not be zero length");
            }
            this.bucketType = bucketType.getBytes(Charset.defaultCharset());
        }
        return this;
    }

    /**
     * Set the bucket type for this RiakObject
     * <p>
     * Riak is character set agnostic. The bucket type is stored as bytes. This method 
     * will convert the provided {@code String} using the provided {@code Charset}.
     * </p>
     * @param bucketType the bucket type as a {@code String}
     * @param charset the {@link Charset} to use 
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketType} is {@code null} or zero length
     */
    public RiakObject setBucketType(String bucketType, Charset charset) 
    {
        if (bucketType != null) 
        {
            if (bucketType.isEmpty())
            {
                throw new IllegalArgumentException("Bucket type can not be zero length");
            }
            this.bucketType = bucketType.getBytes(charset);
        }
        return this;
    }
        
    /**
     * Set the bucket type for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket type is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param bucketType a {@code byte[]} to be copied and used as the bucket type
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketType} is {@code null} or zero length
     */
    public RiakObject setBucketType(byte[] bucketType)
    {
        if (bucketType != null)
        {
            if (bucketType.length == 0)
            {
                throw new IllegalArgumentException("bucket type can not be zero length");
            }
            this.bucketType = Arrays.copyOf(bucketType, bucketType.length);
        }
        return this;
    }
    
    /**
     * Set the bucket type for this RiakObject.
     * <p>
     * Riak is character set agnostic. The bucket type is stored as bytes. 
     * This method allows for raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is not copied and the reference is used
     * directly. Retaining a reference to this array and making subsequent
     * changes will lead to undefined behavior in regard to thread safety and
     * visibility. 
     * @param bucketType a {@code byte[]} to be used as the bucket type
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code bucketType} is {@code null} or zero length
     */
    public RiakObject unsafeSetBucketType(byte[] bucketType)
    {
        if (bucketType != null && bucketType.length == 0)
        {
            throw new IllegalArgumentException("bucket type can not be zero length");
        }
        this.bucketType = bucketType;
        return this;
    }
    
    // Methods For dealing with Value
    
    /**
     * Returns whether or not this RiakObject has a value
     * @return true if the value has been set, false otherwise
     */
    public boolean hasValue()
    {
        return value != null;
    }
    
    /**
     * Returns the value of this RiakObject as a String
     * <p>
     * The value will be coerced to a {@code String} using the 
     * object's {@code Charset} determined from the content type. If no 
     * character set is present in the content type, UTF-8 is used.
     * </p>
     * @return the internal {@code byte[]} coerced to a {@code String} using the object's {@code Charset} 
     * or {@code null} if the value has not been set.
     */
    public String getValueAsString()
    {
        if (value != null) 
        {
            return new String(value, CharsetUtils.getCharset(contentType));
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the value of this RiakObject as a ByteArrayInputStream
     * @return a {@code ByteArrayInputStream} backed by the internal {@code byte[]} 
     * or {@code null} if the value has not been set.
     */
    public ByteArrayInputStream getValue()
    {
        if (value != null)
        {
            return new ByteArrayInputStream(value);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Returns the value for this RiakObject as raw bytes.
     * @riak.threadsafety A copy of the internal <code>byte[]</code> is returned.
     * @return a copy of the internal {@code byte[]}
     * or {@code null} if the value has not been set. 
     */
    public byte[] getValueAsBytes()
    {
        if (value != null)
        {
            return Arrays.copyOf(value, value.length);
        }
        else 
        {
            return null;
        }
    }
    
    /**
     * Returns the value for this RiakObject as raw bytes.
     * 
     * @riak.threadsafety This method exposes the internal <code>byte[]</code> directly.
     * Modifying the contents of this array will lead to undefined behavior in 
     * regard to thread safety and visibility.
     * @return the internal {@code byte[]} or {@code null} if the value has not been set.
     */
    public byte[] unsafeGetValueAsBytes()
    {
        return value;
    }
    
    /**
     * Set the value for this RiakObject
     * <p>
     * The data payload is stored in Riak as bytes. This method allows for 
     * raw bytes to be used directly.
     * </p>
     * <p>
     * Note that if {@link RiakObject#setContentType(java.lang.String)} is not 
     * called a value of {@value #DEFAULT_CONTENT_TYPE} is used.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param value the {@code byte[]} to be copied and used as the value
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code value} is zero length
     */
    public RiakObject setValue(byte[] value)
    {
        if (value != null && value.length == 0)
        {
            throw new IllegalArgumentException("value can not be zero length");
        }
        this.value = Arrays.copyOf(value, value.length);
        return this;
    }
        
    /**
     * Set the value for this RiakObject 
     * <p>
     * The provided {@code String} will be converted to a {@code byte[]} using 
     * the UTF-8 character set. The character set for this {@code RiakObject} will be 
     * set as if {@link RiakObject#setCharset(java.lang.String)} had been used.
     * </p>
     * @param value a UTF-8 encoded {@code String}
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code value} is zero length
     * @see RiakObject#setCharset(java.lang.String) 
     */
    public RiakObject setValue(String value)
    {
        if (value.isEmpty())
        {
            throw new IllegalArgumentException("Value can not be zero length");
        }
        this.value = value.getBytes(Charset.forName("UTF-8"));
        this.contentType = CharsetUtils.addUtf8Charset(contentType);
        return this;
    }

    /**
     * Set the value for this RiakObject
     * <p>
     * The provided {@code String} will be converted to a {@code byte[]} using 
     * the {@code Charset} specified. The character set for this {@code RiakObject} will be 
     * set as if {@link RiakObject#setCharset(java.lang.String)} had been used.
     * </p>
     * @param value the value for this RiakObject as a {@code String}
     * @param charset - {@code Charset} used to convert {@code value}
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code value} is zero length
     * @see RiakObject#setCharset(java.lang.String) 
     */
    public RiakObject setValue(String value, Charset charset) throws UnsupportedEncodingException
    {
        if (value.isEmpty())
        {
            throw new IllegalArgumentException("Value can not be zero length");
        }
        this.value = value.getBytes(charset);
        this.contentType = CharsetUtils.addCharset(charset, contentType);
        return this;
    }
    
    /**
     * Set the value for this RiakObject
     * <p>
     * The data payload is stored in Riak as bytes. This method allows for 
     * raw bytes to be used directly.
     * </p>
     * @riak.threadsafety The supplied <code>byte[]</code> is not copied and the reference is used
     * directly. Retaining a reference to this array and making subsequent
     * changes will lead to undefined behavior in regard to thread safety and visibility. 
     * @param value the {@code byte[]} to be used as the value
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code value} is zero length
     */
    public RiakObject unsafeSetValue(byte[] value)
    {
        if (value != null && value.length == 0)
        {
            throw new IllegalArgumentException("Value can not be zero length");
        }
        this.value = value;
        return this;
    }
    
    
    /**
     * Return the VClock for this RiakObject
     * <p>
     * Return the vector clock for this {@code RiakObject}.
     * </p>
     * @return the {@link VClock} for this object or {@code null} if it has not been set.
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/">Vector Clocks on the Basho Wiki</a>
     */
    public VClock getVClock()
    {
        return vclock;
    }
    
    /**
     * Set the VClock for this RiakObject
     * @riak.threadsafety The supplied <code>byte[]</code> is copied.
     * @param vclock the {@code byte[]} representing a vector clock to be copied and used
     * @return a reference to this object
     * @throws IllegalArgumentException if {@code vclock} is null or zero length
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/">Vector Clocks on the Basho Wiki</a>
     */
    public RiakObject setVClock(byte[] vclock)
    {
        if (null == vclock || vclock.length == 0)
        {
            throw new IllegalArgumentException("vclock can not be null or zero length");
        }
        this.vclock = new BasicVClock(vclock);
        return this;
    }

     /**
     * Set the VClock for this RiakObject
     * @param vclock the {@link VClock} representing a vector clock to be used
     * @return a reference to this object
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/">Vector Clocks on the Basho Wiki</a>
     */
    public RiakObject setVClock(VClock vclock)
    {
        this.vclock = vclock;
        return this;
    }
    
    /**
     * Returns the version tag (if it is one of a set of siblings) for this RiakObject
     * 
     * @return the vtag if present, otherwise {@code null}
     */
    public String getVtag()
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
     * @return The last modified time or {@code 0} if it has not been set.
     * @see RiakObject#setLastModified(long) 
     */
    public long getLastModified()
    {
        return lastModified;
    }
    
    /**
     * A {@code long} timestamp of milliseconds since the epoch to set as
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
     * @return a {@code String} representing the content type of this object's value
     * @see RiakObject#setValue(java.lang.String) 
     * @see RiakObject#setValue(java.lang.String, java.nio.charset.Charset) 
     * @see RiakObject#setValue(byte[]) 
     * @see RiakObject#unsafeSetValue(byte[])  
     */
    public String getContentType()
    {
        return contentType;
    }

    /**
     * Set the content type of the data payload (value) of the this RaikObject
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header. 
     * </p>
     * @param contentType a {@code String} representing the content type of this object's value
     * @return a reference to this object
     * @see RiakObject#setValue(java.lang.String) 
     * @see RiakObject#setValue(java.lang.String, java.nio.charset.Charset) 
     * @see RiakObject#setValue(byte[]) 
     * @see RiakObject#unsafeSetValue(byte[]) 
     */
    public RiakObject setContentType(String contentType)
    {
        this.contentType = contentType;
        return this;
    }
    
    /**
     * Get the character set for this RiakObject's content (value)
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header. 
     * </p>
     * @return The character set {@code String}
     * @see RiakObject#setCharset(java.lang.String) 
     * @see RiakObject#setValue(java.lang.String) 
     * @see RiakObject#setValue(java.lang.String, java.nio.charset.Charset) 
     * @see RiakObject#setValue(byte[]) 
     * @see RiakObject#unsafeSetValue(byte[]) 
     */
    public String getCharset()
    {
        return CharsetUtils.getDeclaredCharset(contentType);
    }
    
    /**
     * Set the character set for this object's content (value).  
     * <p>
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP {@code Content-Type} header. 
     * </p>
     * @param charset the {@link Charset} to be set
     * @return a reference to this object
     * @see RiakObject#setValue(java.lang.String) 
     * @see RiakObject#setValue(java.lang.String, java.nio.charset.Charset) 
     * @see RiakObject#setValue(byte[]) 
     * @see RiakObject#unsafeSetValue(byte[]) 
     */
    public RiakObject setCharset(String charset)
    {
        this.contentType = CharsetUtils.addCharset(charset, contentType);
        return this;
    }
    
    // Indexes
    
    /**
     * Returns whether this RiakObject has secondary indexes (2i)
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
     * @riak.threadsafety The returned <code>RiakIndexes</code> object encapsulates any/all 
     * indexes for this <code>RiakObject}</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RaikObject</code>.
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
    
    /**
     * Set the Secondary Indexes (2i) for this RiakObject
     * <p>
     * Any existing indexes are replaced.
     * </p>
     * <p>
     * <b>Note:</b> 2i is not supported by all backends. 
     * </p>
     * @riak.threadsafety The supplied <code>RiakIndexes}</code> object encapsulates any/all 
     * indexes for this <code>RiakObject</code>, is mutable, and thread safe. A defensive copy is not made. 
     * Any subsequent changes are directly applied to this <code>RaikObject</code>.
     * @param indexes A {@link RiakIndexes} object encapsulating any/all indexes for this object
     * @return a reference to this object
     * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/2i/">Riak Secondary Indexes</a>
     */
    public synchronized RiakObject setIndexes(RiakIndexes indexes)
    {
        riakIndexes = indexes;
        return this;
    }
    
    // Links
    /**
     * Returns whether this RiakObject contains links
     * @return {@code true} if links are present, {@code false} otherwise
     */
    public boolean hasLinks()
    {
        return (links != null && !links.isEmpty());
    }
    
    /**
     * Returns the RiakLinks for this RiakObject
     * @riak.threadsafety The returned <code>RiakLinks</code> object encapsulates any/all 
     * links for this <code>RaikObject</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RaikObject</code>.
     * @return the {@link RiakIndexes} that encapsulates all/any links for this {@code RiakObject}
     */
    public synchronized RiakLinks getLinks()
    {
        // Lazy initialization of comtainer
        if (null == links)
        {
            links = new RiakLinks();
        }
        
        return links;
    }
    
    /**
     * Set the RiakLinks for this RiakObject
     * <p>
     * Any existing RiakLinks is replaced.
     * <p>
     * @riak.threadsafety The supplied <code>RiakLinks</code> object encapsulates any/all 
     * links for this <code>RaikObject</code>, is mutable, and thread safe. A defensive copy is not made. 
     * Any subsequent changes are directly applied to this <code>RaikObject</code>.
     * @param links the {@link RiakLinks} that encapsulates any/all links for this {@code RiakObject}
     * @return a reference to this object
     */
    public synchronized RiakObject setLinks(RiakLinks links)
    {
        this.links = links;
        return this;
    }
    
    // User Meta
    
    /**
     * Returns if there are any User Meta entries for this RiakObject
     * @return {@code true} if user meta entries are present, {@code false} otherwise.
     */
    public boolean hasUserMeta()
    {
        return userMeta != null && !userMeta.isEmpty();
    }
    
    /**
     * Returns the User Meta for this RiakObject
     * @riak.threadsafety The returned <code>RiakUserMetadata}</code> encapsulates any/all 
     * user meta entries for this <code>RaikObject</code>, is mutable, and thread safe. Any changes
     * are directly applied to this <code>RaikObject</code>.
     * @return the {@code RiakUserMetadata} that contains any/all User Meta entries for this {@code RiakObject}
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
     * Set the User Meta for this RiakObject
     * <p>
     * Any existing User Meta is replaced.
     * </P>
     * @riak.threadsafety The supplied <code>RiakUserMetadata}</code> object encapsulates any/all 
     * User meta for this <code>RaikObject</code>, is mutable, and thread safe. A defensive copy is not made. 
     * Any subsequent changes are directly applied to this <code>RaikObject</code>.
     * @param userMeta a RiakUserMetadata object containing User metadata entries.
     * @return a reference to this object
     */
    public synchronized RiakObject setUserMeta(RiakUserMetadata userMeta)
    {
        this.userMeta = userMeta;
        return this;
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
        
    /**
     * Returns whether or not this RiakObject is marked as being deleted (a tombstone)
     * @return [{@code true} is this {@code RiakObject} is a tombstone, {@code false} otherwise
     */
    public boolean isDeleted()
    {
        return isDeleted;
    }
    
    /**
     * Marks this RiakObject as being modified or not in Riak
     * 
     * @param isModified
     * @return a reference to this object
     */
    public RiakObject setModified(boolean isModified)
    {
        this.isModified = isModified;
        return this;
    }
    
    /**
     * Returns whether this RiakObject is marked as having been modified in Riak
     * @return {@code true} if the object has been modified in Riak, {@code false} otherwise.
     */
    public boolean isModified()
    {
        return isModified;
    }
        
    /**
     * Marks this RiakObject as not being found in Riak
     * 
     * @param isNotFound
     * @return a reference to this object
     */
    public RiakObject setNotFound(boolean isNotFound)
    {
        this.isNotFound = isNotFound;
        return this;
    }
    
    /**
     * Returns whether this RiakObject was not found in Riak.
     * @return {@code true} if the object was not found, {@code false} otherwise.
     */
    public boolean isNotFound()
    {
        return isNotFound;
    }

}
