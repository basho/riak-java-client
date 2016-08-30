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

import com.basho.riak.client.core.query.crdt.ops.CounterOp;

/**
 * An update to a Riak counter datatype.
 * <p>
 * When building a {@link UpdateCounter} or {@link UpdateMap} command
 * this class is used to encapsulate the update to be performed on a
 * Riak counter datatype.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class CounterUpdate implements DatatypeUpdate
{
    private long delta = 0;

    /**
     * Constructs a CounterUpdate with a delta of 0 (zero).
     */
    public CounterUpdate()
    {
    }

    /**
     * Constructs a CounterUpdate with the supplied delta.
     * <p>To decrease a counter supply a negative value.</p>
     * @param delta the value to add to the counter in Riak.
     */
    public CounterUpdate(long delta)
    {
        this.delta = delta;
    }

    /**
     * Get the delta.
     * @return the value contained in this CounterUpdate.
     */
    public long getDelta()
    {
        return delta;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public CounterOp getOp()
    {
        return new CounterOp(delta);
    }
}
