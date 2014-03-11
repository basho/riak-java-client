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
package com.basho.riak.client.query.crdt.types;

import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class RiakMap extends RiakDatatype
{

    private final Map<BinaryValue, RiakDatatype> entries =
        new HashMap<BinaryValue, RiakDatatype>();

    public RiakMap(List<MapEntry> entries)
    {
        for (MapEntry entry : entries)
        {
            this.entries.put(entry.field, entry.element);
        }
    }

    public RiakDatatype get(BinaryValue key)
    {
        return entries.get(key);
    }

	public RiakMap getMap(BinaryValue key)
	{
		return entries.get(key).getAsMap();
	}

	public RiakMap getMap(String key)
	{
		return getMap(BinaryValue.create(key));
	}

	public RiakSet getSet(BinaryValue key)
	{
		return entries.get(key).getAsSet();
	}

	public RiakSet getSet(String key)
	{
		return getSet(BinaryValue.create(key));
	}

	public RiakCounter getCounter(BinaryValue key)
	{
		return entries.get(key).getAsCounter();
	}

	public RiakCounter getCounter(String key)
	{
		return getCounter(BinaryValue.create(key));
	}

	public RiakFlag getFlag(BinaryValue key)
	{
		return entries.get(key).getAsFlag();
	}

	public RiakFlag getFlag(String key)
	{
		return getFlag(BinaryValue.create(key));
	}

	public RiakRegister getRegister(BinaryValue key)
	{
		return entries.get(key).getAsRegister();
	}

	public RiakRegister getRegister(String key)
	{
		return getRegister(BinaryValue.create(key));
	}

    /**
     * Get this CrdtMap as a {@link Map}. The returned asMap  is unmodifiable.
     *
     * @return a read-only view of the asMap
     */
    public Map<BinaryValue, RiakDatatype> view()
    {
        return unmodifiableMap(entries);
    }

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
