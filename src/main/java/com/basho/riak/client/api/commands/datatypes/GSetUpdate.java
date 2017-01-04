/*
 * Copyright 2016 Basho Technologies Inc
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

import com.basho.riak.client.core.query.crdt.ops.GSetOp;
import com.basho.riak.client.core.query.crdt.ops.SetOp;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

/**
 * An update to a Riak grow-only set datatype.
 * <p>
 * When building an {@link UpdateSet} command
 * this class is used to encapsulate the update to be performed on a
 * Riak gset datatype.
 * </p>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.2
 */
public class GSetUpdate implements DatatypeUpdate
{
    protected final Set<BinaryValue> adds = new HashSet<>();

    /**
     * Constructs an empty GSetUpdate.
     */
    public GSetUpdate()
    {
    }

    /**
     * Add the provided value to the set in Riak.
     * @param value the value to be added.
     * @return a reference to this object.
     */
    public GSetUpdate add(BinaryValue value)
    {
        this.adds.add(value);
        return this;
    }

    /**
     * Add the provided value to the set in Riak.
     * @param value the value to be added.
     * @return a reference to this object.
     */
    public GSetUpdate add(String value)
    {
        this.adds.add(BinaryValue.create(value));
        return this;
    }

    /**
     * Get the set of additions contained in this update.
     * @return the set of additions.
     */
    public Set<BinaryValue> getAdds()
    {
        return adds;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public GSetOp getOp()
    {
        return new GSetOp(adds);
    }

    @Override
    public String toString()
    {
        return "Add: " + adds;
    }
}
