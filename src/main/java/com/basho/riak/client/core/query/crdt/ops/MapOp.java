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
package com.basho.riak.client.core.query.crdt.ops;

import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class MapOp implements CrdtOp
{
    public enum FieldType
    {
        SET, COUNTER, MAP, REGISTER, FLAG
    }

    public static class MapField
    {
        public final FieldType type;
        public final BinaryValue key;

        public MapField(FieldType type, BinaryValue key)
        {
            this.type = type;
            this.key = key;
        }
    }

    public static class MapUpdate
    {
        public final MapField field;
        public final CrdtOp op;

        public MapUpdate(MapField field, CrdtOp op)
        {
            this.field = field;
            this.op = op;
        }
    }

    private final Set<MapField> removes;
    private final Set<MapUpdate> updates;

    public MapOp()
    {
        removes = new HashSet<>();
        updates = new HashSet<>();
    }

    public MapOp(Set<MapField> removes, Set<MapUpdate> updates)
    {
        this.removes = removes;
        this.updates = updates;
    }

    private MapOp update(BinaryValue key, CrdtOp op, FieldType type)
    {
        MapField field = new MapField(type, key);
        MapUpdate update = new MapUpdate(field, op);
        updates.add(update);
        return this;
    }

    public MapOp update(BinaryValue key, SetOp op)
    {
        return update(key, op, FieldType.SET);
    }

    public MapOp update(BinaryValue key, CounterOp op)
    {
        return update(key, op, FieldType.COUNTER);
    }

    public MapOp update(BinaryValue key, MapOp op)
    {
        return update(key, op, FieldType.MAP);
    }

    public MapOp update(BinaryValue key, RegisterOp op)
    {
        return update(key, op, FieldType.REGISTER);
    }

    public MapOp update(BinaryValue key, FlagOp op)
    {
        return update(key, op, FieldType.FLAG);
    }

    public MapOp remove(BinaryValue key, FieldType type)
    {
        MapField field = new MapField(type, key);
        removes.add(field);
        return this;
    }

    public Set<MapField> getRemoves()
    {
        return unmodifiableSet(removes);
    }

    public Set<MapUpdate> getUpdates()
    {
        return unmodifiableSet(updates);
    }
}
