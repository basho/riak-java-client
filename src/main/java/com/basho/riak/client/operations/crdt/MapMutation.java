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

import com.basho.riak.client.query.crdt.ops.CrdtOp;
import com.basho.riak.client.query.crdt.ops.MapOp;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Set;

public class MapMutation extends CrdtMutation
{

    private final Set<MapOp.MapField> adds = new HashSet<MapOp.MapField>();
    private final Set<MapOp.MapField> removes = new HashSet<MapOp.MapField>();
    private final Set<MapOp.MapUpdate> updates = new HashSet<MapOp.MapUpdate>();

    public MapMutation addCounter(ByteArrayWrapper key)
    {
        adds.add(new MapOp.MapField(MapOp.FieldType.COUNTER, key));
        return this;
    }

    public MapMutation addRegister(ByteArrayWrapper key)
    {
        adds.add(new MapOp.MapField(MapOp.FieldType.REGISTER, key));
        return this;
    }

    public MapMutation addFlag(ByteArrayWrapper key)
    {
        adds.add(new MapOp.MapField(MapOp.FieldType.FLAG, key));
        return this;
    }

    public MapMutation addSet(ByteArrayWrapper key)
    {
        adds.add(new MapOp.MapField(MapOp.FieldType.SET, key));
        return this;
    }

    public MapMutation addMap(ByteArrayWrapper key)
    {
        adds.add(new MapOp.MapField(MapOp.FieldType.MAP, key));
        return this;
    }

    public MapMutation removeCounter(ByteArrayWrapper key)
    {
        removes.add(new MapOp.MapField(MapOp.FieldType.COUNTER, key));
        return this;
    }

    public MapMutation removeRegister(ByteArrayWrapper key)
    {
        removes.add(new MapOp.MapField(MapOp.FieldType.REGISTER, key));
        return this;
    }

    public MapMutation removeFlag(ByteArrayWrapper key)
    {
        removes.add(new MapOp.MapField(MapOp.FieldType.FLAG, key));
        return this;
    }

    public MapMutation removeSet(ByteArrayWrapper key)
    {
        removes.add(new MapOp.MapField(MapOp.FieldType.SET, key));
        return this;
    }

    public MapMutation removeMap(ByteArrayWrapper key)
    {
        removes.add(new MapOp.MapField(MapOp.FieldType.MAP, key));
        return this;
    }

    public MapMutation add(ByteArrayWrapper key, MapMutation builder)
    {
        return update(key, builder);
    }

    public MapMutation add(ByteArrayWrapper key, SetMutation builder)
    {
        return update(key, builder);
    }

    public MapMutation add(ByteArrayWrapper key, CounterMutation builder)
    {
        return update(key, builder);
    }

    public MapMutation add(ByteArrayWrapper key, RegisterMutation builder)
    {
        return update(key, builder);
    }

    public MapMutation add(ByteArrayWrapper key, FlagMutation builder)
    {
        return update(key, builder);
    }

    public MapMutation update(ByteArrayWrapper key, MapMutation builder)
    {
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.MAP, key), builder.getOp()));
        return this;
    }

    public MapMutation update(ByteArrayWrapper key, SetMutation builder)
    {
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.SET, key), builder.getOp()));
        return this;
    }

    public MapMutation update(ByteArrayWrapper key, CounterMutation builder)
    {
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.COUNTER, key), builder.getOp()));
        return this;
    }

    public MapMutation update(ByteArrayWrapper key, RegisterMutation builder)
    {
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.REGISTER, key), builder.getOp()));
        return this;
    }

    public MapMutation update(ByteArrayWrapper key, FlagMutation builder)
    {
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.FLAG, key), builder.getOp()));
        return this;
    }

    @Override
    public CrdtOp getOp()
    {
        return new MapOp(adds, removes, updates);
    }
}
