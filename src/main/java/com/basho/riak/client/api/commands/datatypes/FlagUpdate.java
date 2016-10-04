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

import com.basho.riak.client.core.query.crdt.ops.FlagOp;

/**
 * An update to a Riak flag datatype.
 * <p>
 * When building an {@link UpdateMap} command
 * this class is used to encapsulate the update to be performed on a
 * Riak flag datatype contained in the map. It is used in conjunction with the
 * {@link MapUpdate}.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class FlagUpdate implements DatatypeUpdate
{
    private boolean flag = false;

    /**
     * Construct a FlagUpdate.
     * @param flag true to enable, false to disable.
     */
    public FlagUpdate(boolean flag)
    {
        this.flag = flag;
    }

    /**
     * Construct a FlagUpdate with the default disabled (false) value.
     */
    public FlagUpdate()
    {
    }

    /**
     * Return the value of this FlagUpdate.
     * @return true if enabled, false if disabled.
     */
    public boolean isEnabled()
    {
        return flag;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public FlagOp getOp()
    {
        return new FlagOp(flag);
    }
}
