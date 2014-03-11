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

public abstract class RiakDatatype<T>
{

	public abstract T view();

    public boolean isMap()
    {
        return this instanceof RiakMap;
    }

    public boolean isSet()
    {
        return this instanceof RiakSet;
    }

    public boolean isCounter()
    {
        return this instanceof RiakCounter;
    }

    public boolean isRegister()
    {
        return this instanceof RiakRegister;
    }

    public boolean isFlag()
    {
        return this instanceof RiakFlag;
    }

    public RiakMap getAsMap()
    {
        if (!isMap())
        {
            throw new IllegalStateException("This is not an instance of a CrdtMap");
        }
        return (RiakMap) this;
    }

    public RiakSet getAsSet()
    {
        if (!isSet())
        {
            throw new IllegalStateException("This is not an instance of a CrdtSet");
        }
        return (RiakSet) this;
    }

    public RiakCounter getAsCounter()
    {
        if (!isCounter())
        {
            throw new IllegalStateException("This is not an instance of a CrdtCounter");
        }
        return (RiakCounter) this;
    }

    public RiakRegister getAsRegister()
    {
        if (!isRegister())
        {
            throw new IllegalStateException("This is not an instance of a CrdtRegister");
        }
        return (RiakRegister) this;
    }

    public RiakFlag getAsFlag()
    {
        if (!isFlag())
        {
            throw new IllegalStateException("This is not an instance of a CrdtFlag");
        }
        return (RiakFlag) this;
    }



}
