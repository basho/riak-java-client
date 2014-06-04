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

import java.util.*;

public class RiakSet extends RiakDatatype<Set<BinaryValue>>
{
    private final Set<BinaryValue> elements =
        new HashSet<BinaryValue>();

    public RiakSet(List<BinaryValue> elements)
    {
        this.elements.addAll(elements);
    }

    public Set<BinaryValue> viewAsSet()
    {
        return Collections.unmodifiableSet(elements);
    }

    public boolean contains(BinaryValue element)
    {
        return elements.contains(element);
    }

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
