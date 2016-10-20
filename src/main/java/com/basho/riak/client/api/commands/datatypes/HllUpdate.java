/*
 * Copyright 2016 Basho Technologies, Inc.
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

package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.core.query.crdt.ops.HllOp;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * An update to a Riak HyperLogLog datatype.
 * <p>
 * When building an {@link UpdateHll}
 * this class is used to encapsulate the update to be performed on a
 * Riak HyperLogLog datatype.
 * </p>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.1.0
 */
public class HllUpdate implements DatatypeUpdate
{
    private final Set<BinaryValue> adds = new HashSet<>();

    /**
     * Constructs an empty HllUpdate.
     */
    public HllUpdate()
    {
    }

    /**
     * Add the provided element to the HyperLogLog in Riak.
     * @param element the element to be added.
     * @return a reference to this object.
     */
    public HllUpdate addBinary(BinaryValue element)
    {
        this.adds.add(element);
        return this;
    }

    /**
     * Add the provided element to the HyperLogLog in Riak.
     * @param element the element to be added.
     * @return a reference to this object.
     */
    public HllUpdate add(String element)
    {
        this.adds.add(BinaryValue.create(element));
        return this;
    }

    /**
     * Add the provided elements to the HyperLogLog in Riak.
     * @param elements the elements to be added.
     * @return a reference to this object.
     */
    public HllUpdate addAllBinary(Collection<BinaryValue> elements)
    {
        if (elements == null)
        {
            throw new IllegalArgumentException("Elements cannot be null");
        }

        for (BinaryValue element : elements)
        {
            this.adds.add(element);
        }

        return this;
    }

    /**
     * Add the provided elements to the HyperLogLog in Riak.
     * @param elements the elements to be added.
     * @return a reference to this object.
     */
    public HllUpdate addAll(Collection<String> elements)
    {
        if (elements == null)
        {
            throw new IllegalArgumentException("Elements cannot be null");
        }

        for (String element : elements)
        {
            this.adds.add(BinaryValue.create(element));
        }

        return this;
    }

    /**
     * Get the set of element additions contained in this update.
     * @return the set of additions.
     */
    public Set<BinaryValue> getElementAdds()
    {
        return adds;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public HllOp getOp()
    {
        return new HllOp(adds);
    }

    @Override
    public String toString()
    {
        return "Element Adds: " + adds;
    }
}
