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
 * Basic bean that encapsulates a key, bucket name, and bucket type.
 * <p>
 * Objects in Riak are stored using a combination of bucket type, bucket name,
 * and Key. This class encapsulates these three items and is used with most 
 * client operations.
 * </p>
 * <p>
 * Riak itself is character set agnostic; everything is stored as bytes. The 
 * convenience methods in this class rely on either the default Charset or a 
 * supplied one to convert Strings to byte[].
 * <p>
 * 
 * @author David Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class Location
{
    /**
     * The default bucket type used if none is suppled. 
     * The value is "default"
     */
    public static final BinaryValue DEFAULT_BUCKET_TYPE = 
        BinaryValue.create("default", Charset.forName("UTF-8"));

	private final BinaryValue bucketName;
    private volatile BinaryValue bucketType = DEFAULT_BUCKET_TYPE;
	private volatile BinaryValue key;
    
    /**
     * Construct a new Location with the provided bucket name. 
     * @param bucketName The name of the bucket in Riak
     */
    public Location(BinaryValue bucketName)
    {
        if (null == bucketName || bucketName.length() == 0)
        {
            throw new IllegalArgumentException("Bucket name cannot be null or zero length.");
        }
        this.bucketName = bucketName;
    }
    
    /**
     * Construct a new Location with the provided bucket name. 
     * <p>
     * The supplied String is converted to bytes using the default charset.
     * </p>
     * @param bucketName The name of the bucket in Riak
     */
    public Location(String bucketName)
    {
        this(bucketName, Charset.defaultCharset());
    }
    
    /**
     * Construct a new Location with the provided bucket name. 
     * <p>
     * The supplied String is converted to bytes using the supplied charset.
     * </p>
     * @param bucketName The name of the bucket in Riak
     * @param charset the charset used to convert to bytes.
     */
    public Location(String bucketName, Charset charset)
    {
        if (bucketName == null || bucketName.isEmpty())
        {
            throw new IllegalArgumentException("Bucket name cannot be null or zero length");
        }
        
        this.bucketName = BinaryValue.create(bucketName, charset);
    }
    
    /**
     * Set the key portion of this Location.
     * @param key the key to be used in Riak.
     * @return a reference to this object.
     */
    public Location setKey(BinaryValue key)
    {
        this.key = key;
        return this;
    }
    
    /**
     * Set the key portion of this location.
     * <p>
     * The supplied string is converted to bytes using the default charset.
     * </p>
     * @param key the key for this location
     * @return a reference to this object.
     */
    public Location setKey(String key)
    {
        return setKey(key, Charset.defaultCharset());
    }
    
    /**
     * Set the key portion of this location.
     * <p>
     * The supplied string is converted to bytes using the supplied charset.
     * </p>
     * @param key the key for this location
     * @param charset the charset to use for conversion to bytes
     * @return a reference to this object.
     */
    public Location setKey(String key, Charset charset)
    {
        return setKey(BinaryValue.create(key, charset));
    }
    
    /**
     * Set the bucket type for this Location.
     * <p>
     * If not set, the "default" type is used.
     * </p>
     * @param bucketType the Riak bucket type.
     * @return a reference to this object.
     */
    public Location setBucketType(BinaryValue bucketType)
    {
        if (bucketType == null || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }
        this.bucketType = bucketType;
        return this;
    }
    
    /**
     * Set the bucket type for this Location.
     * <p>
     * The supplied String is converted to bytes using the default charset.
     * </p>
     * <p>
     * If not set, the "default" type is used.
     * </p>
     * @param bucketType the Riak bucket type.
     * @return a reference to this object.
     */
    public Location setBucketType(String bucketType)
    {
        return setBucketType(bucketType, Charset.defaultCharset());
    }
    
    /**
     * Set the bucket type for this Location.
     * <p>
     * The supplied String is converted to bytes using the supplied charset.
     * </p>
     * <p>
     * If not set, the "default" type is used.
     * </p>
     * @param bucketType the Riak bucket type.
     * @param charset the charset used to convert the string to bytes
     * @return a reference to this object.
     */
    public Location setBucketType(String bucketType, Charset charset)
    {
        return setBucketType(BinaryValue.create(bucketType, charset));
    }
    
    /**
     * Return whether or not this location contains a key;
     * @return true if key is present, false otherwise.
     */
    public boolean hasKey()
    {
        return key != null;
    }
    
    /**
     * Returns the key for this location.
     * @return the Riak Key.
     */
    public BinaryValue getKey()
    {
        return key;
    }
    
    /**
     * Get the key for this location as a String.
     * <p>
     * The default character set is used.
     * <p>
     * @return the key for this location as a String
     */
    public String getKeyAsString()
    {
        return key.toString();
    }
    
    /**
     * Get the key for this location as a String.
     * <p>
     * The supplied character set is used.
     * <p>
     * @param charset The Charset used to convert to a String.
     * @return the key for this Location as a String.
     */
    public String getKeyAsString(Charset charset)
    {
        return key.toString(charset);
    }
    
    /**
     * Returns the bucket name for this Location.
     * @return the Riak bucket name.
     */
    public BinaryValue getBucketName()
    {
        return bucketName;
    }
    
    /**
     * Get the bucket name for this location as a String.
     * <p>
     * The default Charset is used.
     * <p>
     * @return the key for this Location as a String.
     */
    public String getBucketNameAsString()
    {
        return bucketName.toString();
    }
    
     /**
     * Get the bucket name for this location as a String.
     * <p>
     * The supplied Charset is used.
     * <p>
     * @param charset the Charset used to convert to a String.
     * @return the key for this Location as a String.
     */
    public String getBucketNameAsString(Charset charset)
    {
        return bucketName.toString(charset);
    }
    
    /**
     * Returns the bucket type for this Location.
     * @return the Riak bucket type.
     */
    public BinaryValue getBucketType()
    {
        return bucketType;
    }
    
    /**
     * Get the bucket type for this location as a String.
     * <p>
     * The default Charset is used to convert to a String.
     * </p>
     * @return The bucket type for this location as a String.
     */
    public String getBucketTypeAsString()
    {
        return bucketType.toString();
    }
    
    /**
     * Get the bucket type for this location as a String.
     * <p>
     * The supplied Charset is used to convert to a String.
     * </p>
     * @param charset The Charset used to convert to a String.
     * @return The bucket type for this location as a String.
     */
    public String getBucketTypeAsString(Charset charset)
    {
        return bucketType.toString(charset);
    }
    
    @Override
	public int hashCode()
	{
		int result = 17;
		result = 37 * result + bucketType.hashCode();
		result = 37 * result + bucketName.hashCode();
		result = 37 * result + key.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == this)
		{
			return true;
		}
        else if (obj == null)
        {
            return false;
        }
        else if (!(obj instanceof Location))
		{
			return false;
		}

		Location other = (Location) obj;

        return ( (key == other.key || (key != null && key.equals(other.key))) &&
             (bucketName == other.bucketName || (bucketName != null && bucketName.equals(other.bucketName))) &&
             (bucketType == other.bucketType || (bucketType != null && bucketType.equals(other.bucketType))) );
    }

	@Override
	public String toString()
	{
		return "{type: " + bucketType + ", bucket: " + bucketName + ", key: " + key + "}";
	}
    
}
