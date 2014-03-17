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
package com.basho.riak.client.query.UserMetadata;

import com.basho.riak.client.util.BinaryValue;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A threadsafe container for user metadata.
 * <p>
 * Arbitrary user metadata can be attached to an object in Riak. These are simply key/value 
 * pairs meaningful outside of the actual value of the object. This container 
 * allows the user to manipulate that data.
 * </p>
 * <p>
 * Data in Riak is character asSet agnostic; it's simply raw bytes. Methods are provided here
 * to either use your default character asSet or supply a specific one to convert to and
 * from {@code String}s.
 * <p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see com.basho.riak.client.query.RiakObject#getUserMeta() 
 */
public class RiakUserMetadata
{
    private final ConcurrentHashMap<BinaryValue, BinaryValue> meta =
        new ConcurrentHashMap<BinaryValue, BinaryValue>();
    
    /**
     * Determine if usermeta is present.
     * @return {@code true} if there are no entries, {@code false} otherwise.
     */
    public boolean isEmpty()
    {
        return meta.isEmpty();
    }
    
    /**
     * Determine if a specific usermeta entry is present.
     * <p>
     * This method uses the default {@code Charset} to convert the supplied key.
     * </p>
     * @param key the metadata key 
     * @return {@code true} if the entry is present, {@code false} otherwise.
     */
    public boolean contains(String key)
    {
        return contains(key, Charset.defaultCharset());
    }
    
    /**
     * Determine if a specific usermeta entry is present.
     * <p>
     * This method uses the supplied {@code Charset} to convert the supplied key.
     * </p>
     * @param key the metadata key 
     * @return {@code true} if the entry is present, {@code false} otherwise.
     */
    public boolean contains(String key, Charset charset)
    {
        return meta.contains(BinaryValue.unsafeCreate(key.getBytes()));
    }
    
    /**
     * Get a user metadata entry.
     * <p>
     * This method and its {@link RiakUserMetadata#put(java.lang.String, java.lang.String) }
     * counterpart use the default {@code Charset} to convert the {@code String}s.
     * </p>
     * @param key the key for the user metadata entry as a {@code String} encoded using the default {@code Charset}
     * @return the value for the entry converted to a {@code String} using the default {@code Charset}
     */
    public String get(String key)
    {
        return get(key, Charset.defaultCharset());
    }
    
    /**
     * Get a user metadata entry.
     * <p>
     * This method and its {@link RiakUserMetadata#put(java.lang.String, java.lang.String, java.nio.charset.Charset)  }
     * counterpart use the supplied {@code Charset} to convert the {@code String}s.
     * </p>
     * @param key the key for the user metadata entry as a {@code String} encoded using the supplied {@code Charset}  
     * @return the value for the entry converted to a {@code String} using the supplied {@code Charset}
     */
    public String get(String key, Charset charset)
    {
        BinaryValue wrappedKey = BinaryValue.unsafeCreate(key.getBytes(charset));
        BinaryValue value = meta.get(wrappedKey);
        if (value != null)
        {
            return value.toString(charset);
        }
        else
        {
            return null;
        }

    }
    
    /**
     * Get a user metadata entry.
     * <p>
     * This method and its {@link RiakUserMetadata#put(com.basho.riak.client.util.BinaryValue, com.basho.riak.client.util.BinaryValue)}
     * allow access to the raw bytes.
     * </p>
     * @param key the key for the user metadata entry  
     * @return the value for the entry 
     */
    public BinaryValue get(BinaryValue key)
    {
        return meta.get(key);
    }
    
    /**
     * Get the user metadata entries
     * <p>
     * This method allows access to the user metadata entries directly as raw bytes. The 
     * {@code Set} is an unmodifiable view of all the entries. 
     * <p>
     * @return an unmodifiable view of all the entries.
     */
    public Set<Map.Entry<BinaryValue, BinaryValue>> getUserMetadata()
    {
        return Collections.unmodifiableSet(meta.entrySet());
    }
    
    /**
     * Set a user metadata entry.
     * <p>
     * This method and its {@link RiakUserMetadata#get(java.lang.String) }
     * counterpart use the default {@code Charset} to convert the {@code String}s.
     * </p>
     * @param key the key for the user metadata entry as a {@code String} encoded using the default {@code Charset}
     * @param value the value for the entry as a {@code String} encoded using the default {@code Charset}
     */
    public void put(String key, String value)
    {
        put(key, value, Charset.defaultCharset());
    }
    
    /**
     * Set a user metadata entry.
     * <p>
     * This method and its {@link RiakUserMetadata#get(java.lang.String, java.nio.charset.Charset)  }
     * counterpart use the supplied {@code Charset} to convert the {@code String}s.
     * </p>
     * @param key the key for the user metadata entry as a {@code String} encoded using the supplied {@code Charset}
     * @param value the value for the entry as a {@code String} encoded using the supplied {@code Charset}
     */
    public void put(String key, String value, Charset charset)
    {
        BinaryValue wrappedKey = BinaryValue.unsafeCreate(key.getBytes(charset));
        BinaryValue wrappedValue = BinaryValue.unsafeCreate(value.getBytes(charset));
        meta.put(wrappedKey, wrappedValue);
    }
    
    /**
     * Set a user metadata entry using raw bytes.
     * <p>
     * This method and its {@link RiakUserMetadata#get(com.basho.riak.client.util.BinaryValue)}
     * counterpart all access to the user metadata raw bytes
     * </p>
     * @param key the key for the user metadata entry 
     * @param value the value for the entry 
     */
    public void put(BinaryValue key, BinaryValue value)
    {
        meta.put(key, value);
    }
    
    public void remove(BinaryValue key)
    {
        meta.remove(key);
    }
    
    public void remove(String key, Charset charset)
    {
        BinaryValue wrappedKey = BinaryValue.unsafeCreate(key.getBytes(charset));
        remove(wrappedKey);
    }
    
    public void remove (String key)
    {
        remove(key, Charset.defaultCharset());
    }
    
    // TODO: deal with charset. Should add to annotation
    public RiakUserMetadata put(Map<String,String> metaMap)
    {
        for (Map.Entry<String,String> e : metaMap.entrySet())
        {
            BinaryValue wrappedKey = BinaryValue.unsafeCreate(e.getKey().getBytes());
            BinaryValue wrappedValue = BinaryValue.unsafeCreate(e.getValue().getBytes());
            meta.put(wrappedKey, wrappedValue);
        }
        return this;
    }
    
    /**
     * Clear all user metadata entries.
     */
    public void clear()
    {
        meta.clear();
    }
    
    /**
     * Get the number of user metadata entries.
     * @return the number of entries
     */
    public int size()
    {
        return meta.size();
    }
    
    
    
}
