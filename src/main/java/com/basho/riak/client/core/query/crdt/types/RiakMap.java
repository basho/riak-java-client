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
package com.basho.riak.client.core.query.crdt.types;

import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Representation of the Riak map datatype.
 * <p>
 * This is an immutable map returned when querying Riak for 
 * a map datatype.
 * </p>
 * 
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RiakMap extends RiakDatatype
{

    private final Map<BinaryValue, List<RiakDatatype>> entries =
        new HashMap<BinaryValue, List<RiakDatatype>>();

    public RiakMap(List<MapEntry> mapEntries)
    {

        for (MapEntry entry : mapEntries)
        {
	        List<RiakDatatype> datatypes;
            if ((datatypes = entries.get(entry.field)) == null)
            {
	            datatypes = new LinkedList<RiakDatatype>();
	            entries.put(entry.field, datatypes);
            }
	        datatypes.add(entry.element);
        }
    }

    /**
     * Returns the value(s) to which the specified key is mapped, or {@literal null} 
     * if the map contains no mapping for the key.
     * @param key key whose associated value(s) is to be returned.
     * @return a List containing one or more datatypes, or null if this map contains no mapping for the key.
     */
    public List<RiakDatatype> get(BinaryValue key)
    {
        return entries.get(key);
    }

    /**
     * Returns a RiakMap to which the specified key is mapped, or {@literal null} 
     * if no RiakMap is present.
     * @param key key whose associated RiakMap is to be returned.
     * @return a RiakMap, or null if one is not present.
     */
	public RiakMap getMap(BinaryValue key)
	{
            if (entries.containsKey(key))
            {
                for (RiakDatatype dt : entries.get(key))
                {
                    if (dt.isMap())
                    {
                        return dt.getAsMap();
                    }
                }
            }
            return null;
	}

    /**
     * Returns a RiakMap to which the specified key is mapped, or {@literal null} 
     * if no RiakMap is present.
     * @param key key whose associated RiakMap is to be returned.
     * @return a RiakMap, or null if one is not present.
     */
	public RiakMap getMap(String key)
	{
		return getMap(BinaryValue.create(key));
	}

    /**
     * Returns a RiakSet to which the specified key is mapped, or {@literal null} 
     * if no RiakSet is present.
     * @param key key whose associated RiakSet is to be returned.
     * @return a RiakSet, or null if one is not present.
     */
	public RiakSet getSet(BinaryValue key)
	{
            if (entries.containsKey(key))
            {
                for (RiakDatatype dt : entries.get(key))
                {
                    if (dt.isSet())
                    {
                        return dt.getAsSet();
                    }
                }
            }
            return null;
	}

    /**
     * Returns a RiakSet to which the specified key is mapped, or {@literal null} 
     * if no RiakSet is present.
     * @param key key whose associated RiakSet is to be returned.
     * @return a RiakSet, or null if one is not present.
     */
	public RiakSet getSet(String key)
	{
		return getSet(BinaryValue.create(key));
	}

    /**
     * Returns a RiakCounter to which the specified key is mapped, or {@literal null} 
     * if no RiakCounter is present.
     * @param key key whose associated RiakCounter is to be returned.
     * @return a RiakCounter, or null if one is not present.
     */
	public RiakCounter getCounter(BinaryValue key)
	{
            if (entries.containsKey(key))
            {
                for (RiakDatatype dt : entries.get(key))
                {
                    if (dt.isCounter())
                    {
                        return dt.getAsCounter();
                    }
                }
            }
            return null;
	}

    /**
     * Returns a RiakCounter to which the specified key is mapped, or {@literal null} 
     * if no RiakCounter is present.
     * @param key key whose associated RiakCounter is to be returned.
     * @return a RiakCounter, or null if one is not present.
     */
	public RiakCounter getCounter(String key)
	{
		return getCounter(BinaryValue.create(key));
	}

    /**
     * Returns a RiakFlag to which the specified key is mapped, or {@literal null} 
     * if no RiakFlag is present.
     * @param key key whose associated RiakFlag is to be returned.
     * @return a RiakFlag, or null if one is not present.
     */
	public RiakFlag getFlag(BinaryValue key)
	{
            if (entries.containsKey(key))
            {
                for (RiakDatatype dt : entries.get(key))
                {
                    if (dt.isFlag())
                    {
                        return dt.getAsFlag();
                    }
                }
            }
            return null;
	}

    /**
     * Returns a RiakFlag to which the specified key is mapped, or {@literal null} 
     * if no RiakFlag is present.
     * @param key key whose associated RiakFlag is to be returned.
     * @return a RiakFlag, or null if one is not present.
     */
	public RiakFlag getFlag(String key)
	{
		return getFlag(BinaryValue.create(key));
	}

    /**
     * Returns a RiakRegister to which the specified key is mapped, or {@literal null} 
     * if no RiakRegister is present.
     * @param key key whose associated RiakRegister is to be returned.
     * @return a RiakRegister, or null if one is not present.
     */
	public RiakRegister getRegister(BinaryValue key)
	{
            if (entries.containsKey(key))
            {
                for (RiakDatatype dt : entries.get(key))
                {
                    if (dt.isRegister())
                    {
                        return dt.getAsRegister();
                    }
                }
            }
            return null;
	}

    /**
     * Returns a RiakRegister to which the specified key is mapped, or {@literal null} 
     * if no RiakRegister is present.
     * @param key key whose associated RiakRegister is to be returned.
     * @return a RiakRegister, or null if one is not present.
     */
	public RiakRegister getRegister(String key)
	{
		return getRegister(BinaryValue.create(key));
	}

    /**
     * Get this RiakMap as a {@link Map}. The returned Map is unmodifiable.
     *
     * @return a read-only view of the RiakMap.
     */
    @Override
    public Map<BinaryValue, List<RiakDatatype>> view()
    {
        return unmodifiableMap(entries);
    }

    /**
     * A RiakMap entry (key/value pair).
     */
    public static final class MapEntry
    {

        private final BinaryValue field;
        private final RiakDatatype element;

        public MapEntry(BinaryValue field, RiakDatatype element)
        {
            this.field = field;
            this.element = element;
        }

        public BinaryValue getField()
        {
            return field;
        }

        public RiakDatatype getElement()
        {
            return element;
        }

    }


}
