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
package com.basho.riak.client.operations.crdt;

import com.basho.riak.client.util.ByteArrayWrapper;

public abstract class RiakDatatype<T>
{

    abstract DatatypeMutation getMutation();

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

    public static RiakMap fetchMap(ByteArrayWrapper key)
    {
        return null;
    }

    public static RiakSet fetchSet(ByteArrayWrapper key)
    {
        return null;
    }

    public static RiakCounter fetchCounter(ByteArrayWrapper key)
    {
        return null;
    }

    public static RiakMap store(ByteArrayWrapper key, RiakMap map)
    {
        return null;
    }

    public static RiakCounter store(ByteArrayWrapper key, RiakCounter counter)
    {
        return null;
    }

    public static RiakSet store(ByteArrayWrapper key, RiakSet set)
    {
        return null;
    }

    public static void main(String... args)
    {
        ByteArrayWrapper key = ByteArrayWrapper.create("key");

        RiakMap datatype = fetchMap(key);

        RiakFlag flag = datatype.getFlag("a");
        flag.disable();
        flag.enable();

        RiakSet set = datatype.getSet("b");
        set.add(ByteArrayWrapper.create("stuff"));

        RiakMap daveMap = new RiakMap();
        daveMap.put("count-things", new RiakCounter());
        daveMap.put("secret", new RiakRegister(ByteArrayWrapper.create("secret")));
        datatype.put("dave", daveMap);

        daveMap.getCounter("count-things").increment(10);

        datatype = store(key, datatype); // store changes and receive converged datatype

    }

}
