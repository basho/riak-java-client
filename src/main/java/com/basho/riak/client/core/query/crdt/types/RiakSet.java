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

import java.util.*;

/**
 * Representation of the Riak set datatype.
 * <p>
 * This is an immutable set returned when querying Riak for a set datatype.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RiakSet extends RiakDatatype
{
    private final Set<BinaryValue> elements =
        new HashSet<BinaryValue>();

    public RiakSet(List<BinaryValue> elements)
    {
        this.elements.addAll(elements);
    }

    /**
     * Check to see if the supplied value exists in this RiakSet.
     * @param element The value to check.
     * @return true if this RiakSet contains the value, false otherwise. 
     */
    public boolean contains(BinaryValue element)
    {
        return elements.contains(element);
    }

    /**
     * Check to see if the supplied value exists in this RiakSet.
     * @param element The value to check.
     * @return true if this RiakSet contains the value, false otherwise. 
     */
    public boolean contains(String element)
    {
        return elements.contains(BinaryValue.create(element));
    }
    
    /**
     * Get this set as a {@link Set}. The returned Set is unmodifiable. 
     * @return a read-only view of this RiakSet.
     */
	@Override
	public Set<BinaryValue> view()
	{
		return Collections.unmodifiableSet(elements);
	}
    
    @Override
    public String toString()
    {
        return elements.toString();
    }
}
