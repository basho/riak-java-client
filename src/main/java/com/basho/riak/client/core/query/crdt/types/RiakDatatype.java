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
 * Base abstract class for all datatypes.
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public abstract class RiakDatatype
{
    public abstract Object view();

    /**
     * Determine if this datatype is a map.
     *
     * @return true if a map, false otherwise.
     */
    public boolean isMap()
    {
        return this instanceof RiakMap;
    }

    /**
     * Determine if this datatype is a set.
     *
     * @return true if a set, false otherwise.
     */
    public boolean isSet()
    {
        return this instanceof RiakSet;
    }

    /**
     * Determine if this datatype is a counter.
     *
     * @return true if a counter, false otherwise.
     */
    public boolean isCounter()
    {
        return this instanceof RiakCounter;
    }

    /**
     * Determine if this datatype is a register.
     *
     * @return true if a register, false otherwise.
     */
    public boolean isRegister()
    {
        return this instanceof RiakRegister;
    }

    /**
     * Determine if this datatype is a flag.
     *
     * @return true if a flag, false otherwise.
     */
    public boolean isFlag()
    {
        return this instanceof RiakFlag;
    }

    /**
     * Get this datatype as a map.
     *
     * @return a RiakMap
     * @throws IllegalStateException if this is not a map.
     */
    public RiakMap getAsMap()
    {
        if (!isMap())
        {
            throw new IllegalStateException("This is not an instance of a CrdtMap");
        }
        return (RiakMap) this;
    }

    /**
     * Get this datatype as a set.
     *
     * @return a RiakSet
     * @throws IllegalStateException if this is not a set.
     */
    public RiakSet getAsSet()
    {
        if (!isSet())
        {
            throw new IllegalStateException("This is not an instance of a CrdtSet");
        }
        return (RiakSet) this;
    }

    /**
     * Get this datatype as a counter.
     *
     * @return a RiakCounter
     * @throws IllegalStateException if this is not a counter.
     */
    public RiakCounter getAsCounter()
    {
        if (!isCounter())
        {
            throw new IllegalStateException("This is not an instance of a CrdtCounter");
        }
        return (RiakCounter) this;
    }

    /**
     * Get this datatype as a register.
     *
     * @return a RiakRegister
     * @throws IllegalStateException if this is not a register.
     */
    public RiakRegister getAsRegister()
    {
        if (!isRegister())
        {
            throw new IllegalStateException("This is not an instance of a CrdtRegister");
        }
        return (RiakRegister) this;
    }

    /**
     * Get this datatype as a flag.
     *
     * @return a RiakFlag
     * @throws IllegalStateException if this is not a flag.
     */
    public RiakFlag getAsFlag()
    {
        if (!isFlag())
        {
            throw new IllegalStateException("This is not an instance of a CrdtFlag");
        }
        return (RiakFlag) this;
    }
}
