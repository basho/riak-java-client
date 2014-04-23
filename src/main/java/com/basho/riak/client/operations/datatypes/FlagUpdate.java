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

import com.basho.riak.client.query.crdt.ops.FlagOp;

/**
 * An update to a Riak flag datatype. Flags can only exist within Maps so you must
 * nest a FlagUpdate within an enclosing MapUpdate.
 * <p/>
 * <p/>
 * Usage:
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate().update("flag",
 *       new FlagUpdate(false)))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 * <p/>
 * If a flag is deeply nested within a map (of maps (of maps (...))) you must nest the update appropriately.
 * <p/>
 * <pre>
 *   {@code
 *   Location loc = ...;
 *   RiakClient client = ...;
 *   UpdateMap update = new MapUpdate.Builder(loc,
 *     new MapUpdate().update("map",
 *       new MapUpdate().update("flag",
 *         new FlagUpdate(false))))
 *   .build();
 *   UpdateMap.Response response = client.execute(update);
 *   }
 * </pre>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class FlagUpdate implements DatatypeUpdate
{

	private boolean flag = false;

	/**
	 * Construct an empty flag update
	 */
	public FlagUpdate()
	{
	}

	/**
	 * Construct a new flag update to set the flag to the given value
	 *
	 * @param flag the new value of the flag
	 */
	public FlagUpdate(boolean flag)
	{
		this.flag = flag;
	}

	/**
	 * Set the value of the flag
	 *
	 * @param flag the value
	 * @return this
	 */
	public FlagUpdate set(boolean flag)
	{
		this.flag = flag;
		return this;
	}

	/**
	 * True if the value is true
	 *
	 * @return the value of the flag
	 */
	public boolean isEnabled()
	{
		return flag;
	}

	@Override
	public FlagOp getOp()
	{
		return new FlagOp(flag);
	}
}
