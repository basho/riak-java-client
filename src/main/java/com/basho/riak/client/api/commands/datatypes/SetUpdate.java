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
package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.core.query.crdt.ops.SetOp;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

/**
 * An update to a Riak set datatype.
 * <p>
 * When building an {@link UpdateSet} or {@link UpdateMap} command
 * this class is used to encapsulate the update to be performed on a
 * Riak set datatype.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class SetUpdate implements DatatypeUpdate
{

    private final Set<BinaryValue> adds = new HashSet<>();
    private final Set<BinaryValue> removes = new HashSet<>();

    /**
     * Constructs an empty SetUpdate.
     */
    public SetUpdate()
    {
    }

    /**
     * Add the provided value to the set in Riak.
     * @param value the value to be added.
     * @return a reference to this object.
     */
    public SetUpdate add(BinaryValue value)
    {
        this.adds.add(value);
        return this;
    }

    /**
     * Add the provided value to the set in Riak.
     * @param value the value to be added.
     * @return a reference to this object.
     */
    public SetUpdate add(String value)
    {
        this.adds.add(BinaryValue.create(value));
        return this;
    }

    /**
     * Remove the provided value from the set in Riak.
     * @param value the value to be removed.
     * @return a reference to this object.
     */
    public SetUpdate remove(BinaryValue value)
    {
        this.removes.add(value);
        return this;
    }

    /**
     * Remove the provided value from the set in Riak.
     * @param value the value to be removed.
     * @return a reference to this object.
     */
    public SetUpdate remove(String value)
    {
        this.removes.add(BinaryValue.create(value));
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
     * Get the set of removes contained in this update.
     * @return the set of removes.
     */
    public Set<BinaryValue> getRemoves()
    {
        return removes;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public SetOp getOp()
    {
        return new SetOp(adds, removes);
    }

    @Override
    public String toString()
    {
        return "Add: " + adds + " Remove: " + removes;
    }
}
