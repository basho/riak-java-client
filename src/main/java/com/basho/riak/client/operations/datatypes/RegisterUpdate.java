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

import com.basho.riak.client.query.crdt.ops.RegisterOp;
import com.basho.riak.client.util.BinaryValue;

/**
 * An update to a Riak register datatype. Registers can only exist within Maps so you must
 * nest a RegisterUpdate within an enclosing MapUpdate.
 * <p/>
 * <p/>
 * Usage:
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate().update("flag",
 *       new RegisterUpdate(new byte[] {'/0'})))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 * <p/>
 * If a register is deeply nested within a map (of maps (of maps (...))) you must nest the update appropriately.
 * <p/>
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate().update("map",
 *       new MapUpdate().update("flag",
 *         new RegisterUpdate(new byte[] {'/0'}))))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RegisterUpdate implements DatatypeUpdate
{

	private byte[] value = null;

	public RegisterUpdate(byte[] value)
	{
		this.value = value;
	}

	/**
	 * Create an empty register update
	 */
	public RegisterUpdate()
	{
	}

	/**
	 * Construct a new update for a register
	 *
	 * @param value the value to store
	 * @return this
	 */
	public RegisterUpdate set(byte[] value)
	{
		this.value = value;
		return this;
	}

	/**
	 * Clear the register
	 *
	 * @return this
	 */
	public RegisterUpdate clear()
	{
		this.value = null;
		return this;
	}

	/**
	 * Get the value of this update
	 *
	 * @return
	 */
	public byte[] get()
	{
		return value;
	}

	@Override
	public RegisterOp getOp()
	{
		return new RegisterOp(BinaryValue.create(value));
	}


}
