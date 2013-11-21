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
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RiakMap extends RiakDatatype<Map<ByteArrayWrapper, RiakDatatype<?>>>
{

    private final Map<ByteArrayWrapper, RiakDatatype<?>> map;
    private final MapMutation mutation;

    public RiakMap()
    {
        this(new CrdtMap((List<CrdtMap.MapEntry>) Collections.EMPTY_MAP), new MapMutation());
    }

    public RiakMap(CrdtMap map)
    {
        this(map, new MapMutation());
    }

    RiakMap(CrdtMap map, MapMutation mutation)
    {
        this.map = new HashMap<ByteArrayWrapper, RiakDatatype<?>>();
        for (Map.Entry<ByteArrayWrapper, CrdtElement> e : map.viewAsMap().entrySet())
        {
            RiakDatatype<?> dt;
            ByteArrayWrapper key = e.getKey();
            CrdtElement value = e.getValue();
            if (value.isSet())
            {
                SetMutation m = new SetMutation();
                CrdtSet crdtSet = value.getAsSet();
                this.map.put(key, new RiakSet(crdtSet, m));
            }
            else if (value.isCounter())
            {
                CounterMutation m = new CounterMutation();
                CrdtCounter crdtCounter = value.getAsCounter();
                this.map.put(key, new RiakCounter(crdtCounter, m));
            }
            else if (value.isMap())
            {
                MapMutation m = new MapMutation();
                CrdtMap crdtMap = value.getAsMap();
                this.map.put(key, new RiakMap(crdtMap, m));
            }
            else if (value.isFlag())
            {
                FlagMutation m = new FlagMutation();
                CrdtFlag crdtFlag = value.getAsFlag();
                this.map.put(key, new RiakFlag(crdtFlag, m));
            }
            else if (value.isRegister())
            {
                RegisterMutation m = new RegisterMutation();
                CrdtRegister crdtRegister = value.getAsRegister();
                this.map.put(key, new RiakRegister(crdtRegister, m));
            }

        }
        this.mutation = mutation;
    }

    public RiakSet getSet(ByteArrayWrapper key)
    {
        return (RiakSet) map.get(key);
    }

    public RiakSet getSet(String key)
    {
        return getSet(ByteArrayWrapper.create(key));
    }

    public RiakMap getMap(ByteArrayWrapper key)
    {
        return (RiakMap) map.get(key);
    }

    public RiakMap getMap(String key)
    {
        return getMap(ByteArrayWrapper.create(key));
    }

    public RiakRegister getRegister(ByteArrayWrapper key)
    {
        return (RiakRegister) map.get(key);
    }

    public RiakRegister getRegister(String key)
    {
        return getRegister(ByteArrayWrapper.create(key));
    }

    public RiakCounter getCounter(ByteArrayWrapper key)
    {
        return (RiakCounter) map.get(key);
    }

    public RiakCounter getCounter(String key)
    {
        return getCounter(ByteArrayWrapper.create(key));
    }

    public RiakFlag getFlag(ByteArrayWrapper key)
    {
        return (RiakFlag) map.get(key);
    }

    public RiakFlag getFlag(String key)
    {
        return getFlag(ByteArrayWrapper.create(key));
    }

    public void put(ByteArrayWrapper key, RiakMap map)
    {
        this.map.put(key, map);
        mutation.add(key, map.getMutation());
    }

    public void put(String key, RiakMap map)
    {
        put(ByteArrayWrapper.create(key), map);
    }

    public void put(ByteArrayWrapper key, RiakSet set)
    {
        map.put(key, set);
        mutation.add(key, set.getMutation());
    }

    public void put(String key, RiakSet set)
    {
        put(ByteArrayWrapper.create(key), set);
    }

    public void put(ByteArrayWrapper key, RiakCounter counter)
    {
        map.put(key, counter);
        mutation.add(key, counter.getMutation());
    }

    public void put(String key, RiakCounter counter)
    {
        put(ByteArrayWrapper.create(key), counter);
    }

    public void put(ByteArrayWrapper key, RiakRegister register)
    {
        map.put(key, register);
        mutation.add(key, register.getMutation());
    }

    public void put(String key, RiakRegister register)
    {
        put(ByteArrayWrapper.create(key), register);
    }

    public void put(ByteArrayWrapper key, RiakFlag flag)
    {
        map.put(key, flag);
        mutation.add(key, flag.getMutation());
    }

    public void put(String key, RiakFlag flag)
    {
        put(ByteArrayWrapper.create(key), flag);
    }

    public void remove(ByteArrayWrapper key)
    {
        if (map.containsKey(key))
        {
            RiakDatatype<?> dt = map.remove(key);
            if (dt.isRiakSet())
            {
                mutation.removeSet(key);
            }
            else if (dt.isRiakCounter())
            {
                mutation.removeCounter(key);
            }
            else if (dt.isRiakFlag())
            {
                mutation.removeFlag(key);
            }
            else if (dt.isRiakMap())
            {
                mutation.removeMap(key);
            }
            else if (dt.isRiakRegister())
            {
                mutation.removeRegister(key);
            }
        }
    }

    public void remove(String key)
    {
        remove(ByteArrayWrapper.create(key));
    }

    @Override
    public Map<ByteArrayWrapper, RiakDatatype<?>> view()
    {
        return Collections.unmodifiableMap(map);
    }

    @Override
    MapMutation getMutation()
    {
        return mutation;
    }
}
