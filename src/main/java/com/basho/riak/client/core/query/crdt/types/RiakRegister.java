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

/**
 * Representation of the Riak register datatype.
 * <p>
 * This is an immutable register which can be returned within a {@link RiakMap}.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RiakRegister extends RiakDatatype
{
    private final BinaryValue value;

    public RiakRegister(BinaryValue value)
    {
        this.value = value;
    }

    /**
     * Returns the RiakRegister as a BinaryValue.
     *
     * @return the register.
     */
    public BinaryValue getValue()
    {
        return value;
    }

    /**
     * Returns the RiakRegister as a BinaryValue.
     *
     * @return the register.
     */
    @Override
    public BinaryValue view()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return value.toString();
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

        RiakRegister that = (RiakRegister) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode()
    {
        return value != null ? value.hashCode() : 0;
    }
}
