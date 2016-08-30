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

/**
 * Representation of the Riak flag datatype.
 * <p>
 * This in an immutable boolean value which can be returned within a
 * {@link RiakMap}.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RiakFlag extends RiakDatatype
{
    private boolean enabled = false;

    public RiakFlag(boolean enabled)
    {
        this.enabled = enabled;
    }

    /**
     * Return whether this flag is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean getEnabled()
    {
        return enabled;
    }

    /**
     * Return whether this flag is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    @Override
    public Boolean view()
    {
        return enabled;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        RiakFlag riakFlag = (RiakFlag) o;

        return enabled == riakFlag.enabled;
    }

    @Override
    public int hashCode()
    {
        return (enabled ? 1 : 0);
    }
}
