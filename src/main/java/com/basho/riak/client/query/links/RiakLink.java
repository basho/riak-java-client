/*
 * Copyright 2013 Basho TEchnologies Inc.
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
package com.basho.riak.client.query.links;

import com.basho.riak.client.util.ByteArrayWrapper;
import java.nio.charset.Charset;

/**
 * Models a link from one object to another in Riak. 
 * <p> 
 * Links are metadata that establish one-way relationships between objects in Riak. 
 * They can be used to loosely model graph like relationships between objects in Riak.
 * </p>
 * <p>
 * Data in Riak is character asSet agnostic; it's simply raw bytes. Methods are provided here
 * to either use your default character asSet or supply a specific one to convert to and
 * from {@code String}s.
 * <p>
 *
 * @author Russel Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
 */
public class RiakLink
{

    private final ByteArrayWrapper bucket;
    private final ByteArrayWrapper key;
    private final ByteArrayWrapper tag;

    /**
     * Create a RiakLink from the specified parameters.
     * <p>
     * The values are stored internally as bytes. The default {@code Charset} will
     * be used to convert the supplied {@code String}s
     * <p>
     * @param bucket the bucket name
     * @param key the key
     * @param tag the link tag
     */
    public RiakLink(String bucket, String key, String tag)
    {
        this(bucket, key, tag, Charset.defaultCharset());
    }

    /**
     * Create a RiakLink from the specified parameters.
     * <p>
     * The values are stored internally as bytes. The supplied {@code Charset} will
     * be used to convert the supplied {@code String}s
     * </p>
     * @param bucket the bucket
     * @param key the key
     * @param tag the link tag
     * @param charset the character asSet for the supplied {@code String}s
     */
    public RiakLink(String bucket, String key, String tag, Charset charset)
    {
        this.bucket = ByteArrayWrapper.unsafeCreate(bucket.getBytes(charset));
        this.key = ByteArrayWrapper.unsafeCreate(key.getBytes(charset));
        this.tag = ByteArrayWrapper.unsafeCreate(tag.getBytes(charset));
    }
    
    /**
     * Create a RiakLink from the specified parameters.
     * @param bucket the bucket name
     * @param key the key
     * @param tag the link tag
     */
    public RiakLink(ByteArrayWrapper bucket, ByteArrayWrapper key, ByteArrayWrapper tag)
    {
        this.bucket = bucket;
        this.key = key;
        this.tag = tag;
    }
    
    /**
     * Create a RiakLink that is a copy of another RiakLink.
     *
     * @param riakLink the RiakLink to copy
     */
    public RiakLink(final RiakLink riakLink)
    {
        this.bucket = riakLink.bucket;
        this.key = riakLink.key;
        this.tag = riakLink.tag;
    }

    /**
     * Get the bucket for this RiakLink as a String
     * </p>
     * The bucket is stored internally as bytes. The default {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the bucket as a {@code String} encoded using the default {@code Charset}
     */
    public String getBucket()
    {
        return bucket.toString();
    }
    
    /**
     * Get the bucket for this RiakLink as a String
     * </p>
     * The bucket is stored internally as bytes. The supplied {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the bucket as a {@code String} encoded using the supplied {@code Charset}
     */
    public String getBucket(Charset charset)
    {
        return bucket.toString(charset);
    }
    
    /**
     * Get the bucket as a wrapped byte array
     * @return the bucket name in a {@link ByteArrayWrapper}
     */
    public ByteArrayWrapper getBucketAsBytes()
    {
        return bucket;
    }
    
    /**
     * Get the key for this RiakLink as a String
     * </p>
     * The key is stored internally as bytes. The default {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the key as a {@code String} encoded using the default {@code Charset}
     */
    public String getKey()
    {
        return key.toString();
    }
    
    /**
     * Get the key for this RiakLink as a String
     * </p>
     * The key is stored internally as bytes. The supplied {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the key as a {@code String} encoded using the supplied {@code Charset}
     */
    public String getKey(Charset charset)
    {
        return key.toString(charset);
    }
    
    /**
     * Return the key as a wrapped byte array
     * @return the key in a {@link ByteArrayWrapper}
     */
    public ByteArrayWrapper getKeyAsBytes()
    {
        return key;
    }
    
    /**
     * Get the tag for this RiakLink as a String
     * </p>
     * The tag is stored internally as bytes. The default {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the tag as a {@code String} encoded using the default {@code Charset}
     */
    public String getTag()
    {
        return tag.toString();
    }

    /**
     * Get the tag for this RiakLink as a String
     * </p>
     * The tag is stored internally as bytes. The supplied {@code Charset}
     * is used to convert to a {@code String}
     * </p>
     * @return the tag as a {@code String} encoded using the supplied {@code Charset}
     */
    public String getTag(Charset charset)
    {
        return tag.toString(charset);
    }
    
    /**
     * Get the tag as bytes
     * @return the tag in a {@link ByteArrayWrapper}
     */
    public ByteArrayWrapper getTagAsBytes()
    {
        return tag;
    }
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof RiakLink))
        {
            return false;
        }
        RiakLink other = (RiakLink) obj;
        if (bucket == null)
        {
            if (other.bucket != null)
            {
                return false;
            }
        }
        else if (!bucket.equals(other.bucket))
        {
            return false;
        }
        if (key == null)
        {
            if (other.key != null)
            {
                return false;
            }
        }
        else if (!key.equals(other.key))
        {
            return false;
        }
        if (tag == null)
        {
            if (other.tag != null)
            {
                return false;
            }
        }
        else if (!tag.equals(other.tag))
        {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return String.format("RiakLink [tag=%s, bucket=%s, key=%s]", tag, bucket, key);
    }
}
