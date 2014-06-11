/*
 * Copyright 2014 Basho Technologies Inc. 
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

import com.basho.riak.client.util.BinaryValue;
import java.nio.charset.Charset;

/**
 * Encapsulates a Riak bucket type and bucket name.
 * <p>
 * Riak 2.0 introduced bucket types, which form a namespace in Riak when combined with
 * a bucket name. This class encapsulates those two items for use with operations. 
 * </p>
 * <p>
 * Buckets in Riak are automatically created for a type if they do not yet exist. 
 * Bucket types, on the other hand, are not. Anything other than the {@literal default}
 * bucket type must be explicitly created using the {@literal riak_admin} command line tool.
 * </p>
 * <p>
 * Bucket types can only be in UTF-8. Bucket names have no restrictions.
 * </p>
 * <p>
 * Buckets in the default bucket type can not hold CRDTs (e.g. Maps, Counters, Sets, etc).
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class Namespace
{

    
    /**
     * The default bucket type in Riak.
     * The Riak default bucket type. 
     * The type is {@value #DEFAULT_BUCKET_TYPE}
     */
    public static final String DEFAULT_BUCKET_TYPE = "default";
    private static final BinaryValue DEFAULT_TYPE = BinaryValue.createFromUtf8(DEFAULT_BUCKET_TYPE);
    
    private final BinaryValue type;
    private final BinaryValue bucket;
    
    /**
     * Construct a new Namespace with the provided bucket type and name. 
     * @param bucketType The bucket type in Riak. This must be UTF-8 encoded.
     * @param bucketName The bucket in Riak.
     */
    public Namespace(BinaryValue bucketType, BinaryValue bucketName)
    {
        if (null == bucketName || bucketName.length() == 0)
        {
            throw new IllegalArgumentException("Bucket name cannot be null or zero length.");
        }
        else if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type cannot be null or zero length.");
        }
        this.bucket = bucketName;
        this.type = bucketType;
    }
    
    /**
     * Construct a new Namespace with the provided bucket type and name. 
     * <p>
     * The supplied bucket type will be converted to bytes using UTF-8. 
     * </p>
     * <p>
     * The supplied bucket name is converted to bytes using the supplied charset.
     * </p>
     * @param bucketType The bucket type in Riak. This must be a valid UTF-8 string.
     * @param bucketName The name of the bucket in Riak.
     * @param charset the charset used to convert the bucket name to bytes.
     */
    public Namespace(String bucketType, String bucketName, Charset charset)
    {
        if (bucketName == null || bucketName.isEmpty())
        {
            throw new IllegalArgumentException("Bucket name cannot be null or zero length");
        }
        else if (bucketType == null || bucketType.isEmpty())
        {
            throw new IllegalArgumentException("Bucket type cannot be null or zero length");
        }
        else if (charset == null)
        {
            throw new IllegalArgumentException("Charset cannot be null");
        }
        
        this.bucket = BinaryValue.create(bucketName, charset);
        this.type = BinaryValue.createFromUtf8(bucketType);
    }
    
    /**
     * Construct a new Namespace with the provided bucket type and name. 
     * <p>
     * The bucket type will be converted to bytes using UTF-8. 
     * </p>
     * <p>
     * The supplied bucketName is converted to bytes using the default charset.
     * </p>
     * @param bucketType The bucket type in Riak.
     * @param bucketName The bucket in Riak
     */
    public Namespace(String bucketType, String bucketName)
    {
        this(bucketType, bucketName, Charset.defaultCharset());
    }
    
    /**
     * Construct a new Namespace with the provided bucket name and the default bucket type.
     * <p>
     * The Namespace will use the default bucket type and the bucket
     * name provided.
     * </p>
     * @param bucketName the bucket name in Riak.
     * @see #DEFAULT_BUCKET_TYPE
     */
    public Namespace(BinaryValue bucketName)
    {
        this(DEFAULT_TYPE, bucketName);
    }
    
    /**
     * Construct a Namespace with the provided bucket name and the default bucket type.
     * <p>
     * The Namespace will use the default bucket type and the bucket
     * name provided.
     * </p>
     * The supplied bucket name will be converted to bytes using the supplied charset.
     * <p>
     * </p>
     * @param bucketName the name of the bucket.
     * @param charset the charset to use to convert the string to bytes.
     * @see #DEFAULT_BUCKET_TYPE
     */
    public Namespace(String bucketName, Charset charset)
    {
        this(DEFAULT_BUCKET_TYPE, bucketName, charset);
    }
    
    /**
     * Construct a Namespace with the provided bucket name and the default bucket type.
     * <p>
     * The Namespace will use the default bucket type and the bucket
     * name provided.
     * </p>
     * The supplied bucket name will be converted to bytes using the default charset.
     * <p>
     * </p>
     * @param bucketName the name of the bucket.
     * @see #DEFAULT_BUCKET_TYPE
     */
    public Namespace(String bucketName)
    {
        this(bucketName, Charset.defaultCharset());
    }
    
    /**
     * Returns the bucket type for this Namespace.
     * @return the Riak bucket type.
     */
    public BinaryValue getBucketType()
    {
        return type;
    }
    
    /**
     * Get the bucket type for this Namespace as a String.
     * <p>
     * The default Charset is used to convert to a String.
     * </p>
     * @return The bucket type for this Namespace as a String.
     */
    public String getBucketTypeAsString()
    {
        return type.toString();
    }
    
    /**
     * Get the bucket type for this Namespace as a String.
     * <p>
     * The supplied Charset is used to convert to a String.
     * </p>
     * @param charset The Charset used to convert to a String.
     * @return The bucket type for this Namespace as a String.
     */
    public String getBucketTypeAsString(Charset charset)
    {
        return type.toString(charset);
    }
    
    /**
     * Returns the bucket name for this Namespace.
     * @return the Riak bucket name.
     */
    public BinaryValue getBucketName()
    {
        return bucket;
    }
    
    /**
     * Get the bucket name for this Namespace as a String.
     * <p>
     * The default Charset is used.
     * <p>
     * @return the key for this Location as a String.
     */
    public String getBucketNameAsString()
    {
        return bucket.toString();
    }
    
     /**
     * Get the bucket name for this Namespace as a String.
     * <p>
     * The supplied Charset is used.
     * <p>
     * @param charset the Charset used to convert to a String.
     * @return the key for this Location as a String.
     */
    public String getBucketNameAsString(Charset charset)
    {
        return bucket.toString(charset);
    }
    
    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 37 * hash + (this.type != null ? this.type.hashCode() : 0);
        hash = 37 * hash + (this.bucket != null ? this.bucket.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final Namespace other = (Namespace) obj;
        if (this.type != other.type && (this.type == null || !this.type.equals(other.type)))
        {
            return false;
        }
        if (this.bucket != other.bucket && (this.bucket == null || !this.bucket.equals(other.bucket)))
        {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString()
    {
        return "{type: " + type + ", bucket: " + bucket + "}";
    }
    
}
