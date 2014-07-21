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

package com.basho.riak.client.core.query;

import com.basho.riak.client.core.util.BinaryValue;
import java.nio.charset.Charset;

/**
 * Encapsulates a key and Namespace.
 * <p>
 * Many client operations locate a piece of information in Riak via a
 * bucket type, bucket name, and  a key. This class encapsulates these 
 * three items by combining a {@link Namespace} with a key and is 
 * used with most client operations.
 * </p>
 * <p>
 * Riak itself is character set agnostic; everything is stored as bytes. The 
 * convenience methods in this class rely on either the default Charset or a 
 * supplied one to convert Strings to a byte[].
 * <p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class Location
{
    
	private final Namespace namespace;
	private final BinaryValue key;
    
    /**
     * Construct a new Location with the provided Namespace and key.
     * @param namespace The namespace for this location.
     * @param key The key for this location.
     */
    public Location(Namespace namespace, BinaryValue key)
    {
        if (namespace == null)
        {
            throw new IllegalArgumentException("Namespace cannot be null");
        }
        else if (key == null || key.length() == 0)
        {
            throw new IllegalArgumentException("Key cannot be null or zero length");
        }
        this.namespace = namespace;
        this.key = key;
    }
    
    /**
     * Construct a new Location with the provided Namespace and key.
     * <p>
     * The supplied string is converted to bytes using the supplied charset.
     * </p>
     * @param namespace The namespace for this location.
     * @param key The key for this location.
     * @param charset the charset for the key
     */
    public Location(Namespace namespace, String key, Charset charset)
    {
        if (key == null || key.length() == 0)
        {
            throw new IllegalArgumentException("Key cannot be null or zero length");
        }
        else if (namespace == null)
        {
            throw new IllegalArgumentException("Namespace cannot be null");
        }
        else if (charset == null)
        {
            throw new IllegalArgumentException("Charset cannot be null");
        }
        this.namespace = namespace;
        this.key = BinaryValue.create(key, charset);
    }
    
    /**
     * Construct a new Location with the provided namespace and key.
     * <p>
     * The supplied string is converted to bytes using the default charset.
     * </p>
     * @param namespace The namespace for this location.
     * @param key The key for this location.
     */
    public Location(Namespace namespace, String key)
    {
        this(namespace, key, Charset.defaultCharset());
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
     * Return the Namespace for this location.
     * @return the namespace.
     */
    public Namespace getNamespace()
    {
        return namespace;
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 79 * hash + (this.namespace != null ? this.namespace.hashCode() : 0);
        hash = 79 * hash + (this.key != null ? this.key.hashCode() : 0);
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
        final Location other = (Location) obj;
        if (this.namespace != other.namespace && (this.namespace == null || !this.namespace.equals(other.namespace)))
        {
            return false;
        }
        if (this.key != other.key && (this.key == null || !this.key.equals(other.key)))
        {
            return false;
        }
        return true;
    }

	@Override
	public String toString()
	{
		return "{namespace: " + namespace + ", key: " + key + "}";
	}
    
}
