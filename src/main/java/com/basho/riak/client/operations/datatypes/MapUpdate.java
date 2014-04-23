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

import com.basho.riak.client.query.crdt.ops.MapOp;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

/**
 * Update a map or one of it's nested datatypes.
 * <p/>
 * Usage:
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate()
 *       .addSet("set)
 *       .removeCounter("counter")
 *       .update("register", new RegisterUpdate("a_value")))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 * <p/>
 * Since maps can have nested maps and other nested datatypes
 * (counters, sets, maps, registers, flags), you must nest your
 * updates accordingly.
 * <p/>
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate().update("map",
 *       new MapUpdate().update("set",
 *         new SetUpdate()
 *           .add("value1")
 *           .add("value2")
 *           .remove("value3"))))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class MapUpdate implements DatatypeUpdate
{

	private final Set<MapOp.MapField> adds = new HashSet<MapOp.MapField>();
	private final Set<MapOp.MapField> removes = new HashSet<MapOp.MapField>();
	private final Set<MapOp.MapUpdate> updates = new HashSet<MapOp.MapUpdate>();

	/**
	 * Construct an empty map update
	 */
	public MapUpdate()
	{
	}

	/**
	 * Add a zero initialized counter to the enclosing map at the given key
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate addCounter(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		adds.add(new MapOp.MapField(MapOp.FieldType.COUNTER, k));
		return this;
	}

	/**
	 * Add an empty register to the enclosing map at the given key
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate addRegister(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		adds.add(new MapOp.MapField(MapOp.FieldType.REGISTER, k));
		return this;
	}

	/**
	 * Add a false initialized flag to the enclosing map at the given key
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate addFlag(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		adds.add(new MapOp.MapField(MapOp.FieldType.FLAG, k));
		return this;
	}

	/**
	 * Add an empty set to the enclosing map at the given key
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate addSet(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		adds.add(new MapOp.MapField(MapOp.FieldType.SET, k));
		return this;
	}

	/**
	 * Add an empty map to the enclosing map at the given key
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate addMap(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		adds.add(new MapOp.MapField(MapOp.FieldType.MAP, k));
		return this;
	}

	/**
	 * Remove a counter at the given key from the enclosing map
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate removeCounter(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		removes.add(new MapOp.MapField(MapOp.FieldType.COUNTER, k));
		return this;
	}

	/**
	 * Remove a register at the given key from the enclosing map
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate removeRegister(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		removes.add(new MapOp.MapField(MapOp.FieldType.REGISTER, k));
		return this;
	}

	/**
	 * Remove a flag at the given key from the enclosing map
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate removeFlag(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		removes.add(new MapOp.MapField(MapOp.FieldType.FLAG, k));
		return this;
	}

	/**
	 * Remove a set at the given key from the enclosing map
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate removeSet(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		removes.add(new MapOp.MapField(MapOp.FieldType.SET, k));
		return this;
	}

	/**
	 * Remove a map at the given key from the enclosing map
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate removeMap(String key)
	{
		BinaryValue k = BinaryValue.create(key);
		removes.add(new MapOp.MapField(MapOp.FieldType.MAP, k));
		return this;
	}

	/**
	 * Add or update a map at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate add(String key, MapUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		return update(key, builder);
	}

	/**
	 * Add or update a set at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate add(String key, SetUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		return update(key, builder);
	}

	/**
	 * Add or update a counter at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate add(String key, CounterUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		return update(key, builder);
	}

	/**
	 * Add or update a register at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate add(String key, RegisterUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		return update(key, builder);
	}

	/**
	 * Add or update a flag at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate add(String key, FlagUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		return update(key, builder);
	}

	/**
	 * Add or update a map at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate update(String key, MapUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.MAP, k), builder.getOp()));
		return this;
	}

	/**
	 * Add or update a set at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate update(String key, SetUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.SET, k), builder.getOp()));
		return this;
	}

	/**
	 * Add or update a counter at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate update(String key, CounterUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.COUNTER, k), builder.getOp()));
		return this;
	}

	/**
	 * Add or update a counter at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate update(String key, RegisterUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.REGISTER, k), builder.getOp()));
		return this;
	}

	/**
	 * Add or update a flag at the given key in the enclosing map with the given value
	 *
	 * @param key the map key
	 * @return this
	 */
	public MapUpdate update(String key, FlagUpdate builder)
	{
		BinaryValue k = BinaryValue.create(key);
		updates.add(new MapOp.MapUpdate(new MapOp.MapField(MapOp.FieldType.FLAG, k), builder.getOp()));
		return this;
	}

	@Override
	public MapOp getOp()
	{
		return new MapOp(adds, removes, updates);
	}
}
