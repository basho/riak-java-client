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
package com.basho.riak.client.operations.datatypes;

import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;

public abstract class RiakDatatype<T>
{

    public abstract T view();

    public boolean isRiakMap()
    {
        return this instanceof RiakMap;
    }

    public boolean isRiakSet()
    {
        return this instanceof RiakSet;
    }

    public boolean isRiakCounter()
    {
        return this instanceof RiakCounter;
    }

    public boolean isRiakRegister()
    {
        return this instanceof RiakRegister;
    }

    public boolean isRiakFlag()
    {
        return this instanceof RiakFlag;
    }

    public RiakMap getAsRiakMap()
    {
        return (RiakMap) this;
    }

    public RiakSet getAsRiakSet()
    {
        return (RiakSet) this;
    }

    public RiakCounter getAsRiakCounter()
    {
        return (RiakCounter) this;
    }

    public RiakRegister getAsRiakRegister()
    {
        return (RiakRegister) this;
    }

    public RiakFlag getAsRiakFlag()
    {
        return (RiakFlag) this;
    }


}
