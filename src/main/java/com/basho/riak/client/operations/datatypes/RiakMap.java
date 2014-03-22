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

import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public class RiakMap extends RiakDatatype<Map<byte[], RiakDatatype<?>>>
{

    private final CrdtMap map;

    public RiakMap(CrdtMap map)
    {
        this.map = map;
    }

    public RiakSet getSet(byte[] key)
    {
        return new RiakSet(map.get(BinaryValue.create(key)).getAsSet());
    }

    public RiakSet getSet(String key)
    {
        return new RiakSet(map.get(BinaryValue.create(key)).getAsSet());
    }

    public RiakMap getMap(byte[] key)
    {
        return new RiakMap(map.get(BinaryValue.create(key)).getAsMap());
    }

    public RiakMap getMap(String key)
    {
        return new RiakMap(map.get(BinaryValue.create(key)).getAsMap());
    }

    public RiakRegister getRegister(byte[] key)
    {
        return new RiakRegister(map.get(BinaryValue.create(key)).getAsRegister());
    }

    public RiakRegister getRegister(String key)
    {
        return new RiakRegister(map.get(BinaryValue.create(key)).getAsRegister());
    }

    public RiakCounter getCounter(byte[] key)
    {
        return new RiakCounter(map.get(BinaryValue.create(key)).getAsCounter());
    }

    public RiakCounter getCounter(String key)
    {
        return new RiakCounter(map.get(BinaryValue.create(key)).getAsCounter());
    }

    public RiakFlag getFlag(byte[] key)
    {
        return new RiakFlag(map.get(BinaryValue.create(key)).getAsFlag());
    }

    public RiakFlag getFlag(String key)
    {
        return new RiakFlag(map.get(BinaryValue.create(key)).getAsFlag());
    }

    public Map<byte[], RiakDatatype<?>> view()
    {
        Map<byte[], RiakDatatype<?>> rmap = new HashMap<byte[], RiakDatatype<?>>();
        for (Map.Entry<BinaryValue, CrdtElement> entry : map.viewAsMap().entrySet())
        {
            RiakDatatype<?> datatype = null;
            CrdtElement element = entry.getValue();

            if (element.isCounter())
            {
                datatype = new RiakCounter(element.getAsCounter());
            }
            else if (element.isFlag())
            {
                datatype = new RiakFlag(element.getAsFlag());
            }
            else if (element.isMap())
            {
                datatype = new RiakMap(element.getAsMap());
            }
            else if (element.isRegister())
            {
                datatype = new RiakRegister(element.getAsRegister());
            }
            else if (element.isSet())
            {
                datatype = new RiakSet(element.getAsSet());
            }

            rmap.put(entry.getKey().getValue(), datatype);
        }
        return Collections.unmodifiableMap(rmap);
    }

}
