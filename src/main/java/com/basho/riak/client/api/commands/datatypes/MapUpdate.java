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
package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.core.query.crdt.ops.MapOp;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

/**
 * An update to a Riak map datatype.
 * <p>
 * When building an {@link UpdateMap} command
 * this class is used to encapsulate the update to be performed on a
 * Riak map datatype. It is a composition of other updates.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class MapUpdate implements DatatypeUpdate
{
    private final Set<MapOp.MapField> removes = new HashSet<>();
    private final Set<MapOp.MapUpdate> updates = new HashSet<>();

    /**
     * Construct an empty MapUpdate.
     */
    public MapUpdate()
    {
    }

    /**
     * Update the map in Riak by removing the counter mapped to the provided key.
     * @param key the key the counter is mapped to.
     * @return a reference to this object.
     */
    public MapUpdate removeCounter(String key)
    {
        BinaryValue k = BinaryValue.create(key);
        removes.add(new MapOp.MapField(MapOp.FieldType.COUNTER, k));
        return this;
    }

    /**
     * Update the map in Riak by removing the register mapped to the provided key.
     * @param key the key the register is mapped to.
     * @return a reference to this object.
     */
    public MapUpdate removeRegister(String key)
    {
        BinaryValue k = BinaryValue.create(key);
        removes.add(new MapOp.MapField(MapOp.FieldType.REGISTER, k));
        return this;
    }

    /**
     * Update the map in Riak by removing the flag mapped to the provided key.
     * @param key the key the flag is mapped to.
     * @return a reference to this object.
     */
    public MapUpdate removeFlag(String key)
    {
        BinaryValue k = BinaryValue.create(key);
        removes.add(new MapOp.MapField(MapOp.FieldType.FLAG, k));
        return this;
    }

    /**
     * Update the map in Riak by removing the set mapped to the provided key.
     * @param key the key the set is mapped to.
     * @return a reference to this object.
     */
    public MapUpdate removeSet(String key)
    {
        BinaryValue k = BinaryValue.create(key);
        removes.add(new MapOp.MapField(MapOp.FieldType.SET, k));
        return this;
    }

    /**
     * Update the map in Riak by removing the map mapped to the provided key.
     * @param key the key the map is mapped to.
     * @return a reference to this object.
     */
    public MapUpdate removeMap(String key)
    {
        BinaryValue k = BinaryValue.create(key);
        removes.add(new MapOp.MapField(MapOp.FieldType.MAP, k));
        return this;
    }

    /**
     * Update the map in Riak by adding/updating the map mapped to the provided key.
     * <p>
     * If there is no map referenced by the key, it is created
     * <p>
     *
     * @param key the key the map is mapped to.
     * @param builder the update to apply to the map the key is mapped to. If none exists it is created.
     * @return a reference to this object.
     */
    public MapUpdate update(String key, MapUpdate builder)
    {
        BinaryValue k = BinaryValue.create(key);
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.MAP, k), builder.getOp()));
        return this;
    }

    /**
     * Update the map in Riak by adding/updating the set mapped to the provided key.
     * <p>
     * If there is no set referenced by the key, it is created
     * <p>
     *
     * @param key the key the set is mapped to.
     * @param builder the update to apply to the set the key is mapped to. If none exists it is created.
     * @return a reference to this object.
     */
    public MapUpdate update(String key, SetUpdate builder)
    {
        BinaryValue k = BinaryValue.create(key);
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.SET, k), builder.getOp()));
        return this;
    }

    /**
     * Update the map in Riak by adding/updating the counter mapped to the provided key.
     * <p>
     * If there is no counter referenced by the key, it is created
     * <p>
     *
     * @param key the key the set is mapped to.
     * @param builder the update to apply to the counter the key is mapped to. If none exists it is created.
     * @return a reference to this object.
     */
    public MapUpdate update(String key, CounterUpdate builder)
    {
        BinaryValue k = BinaryValue.create(key);
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.COUNTER, k), builder.getOp()));
        return this;
    }

     /**
     * Update the map in Riak by adding/updating the register mapped to the provided key.
     * <p>
     * If there is no register referenced by the key, it is created
     * <p>
     *
     * @param key the key the register is mapped to.
     * @param builder the update to apply to the register the key is mapped to. If none exists it is created.
     * @return a reference to this object.
     */
    public MapUpdate update(String key, RegisterUpdate builder)
    {
        BinaryValue k = BinaryValue.create(key);
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.REGISTER, k), builder.getOp()));
        return this;
    }

    /**
     * Update the map in Riak by adding/updating the flag mapped to the provided key.
     * <p>
     * If there is no flag referenced by the key, it is created
     * <p>
     *
     * @param key the key the register is mapped to.
     * @param builder the update to apply to the flag the key is mapped to. If none exists it is created.
     * @return a reference to this object.
     */
    public MapUpdate update(String key, FlagUpdate builder)
    {
        BinaryValue k = BinaryValue.create(key);
        updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.FLAG, k), builder.getOp()));
        return this;
    }

    /**
     * Returns the core update.
     * @return the update used by the client core.
     */
    @Override
    public MapOp getOp()
    {
        return new MapOp(removes, updates);
    }
}
