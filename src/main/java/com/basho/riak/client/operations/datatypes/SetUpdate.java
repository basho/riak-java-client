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

import com.basho.riak.client.query.crdt.ops.SetOp;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

/**
 * An update to a Riak set datatype.
 * <p/>
 * Usage:
 * <pre>
 *   {@code
 *   Location loc = null;
 *   RiakClient client = null;
 *   UpdateSet update = new UpdateSet.Builder(loc, new SetUpdate()
 *     .add("value1")
 *     .remove("value2"))
 *   .build();
 *   UpdateSet.Response response = client.execute(update);
 *   }
 * </pre>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class SetUpdate implements DatatypeUpdate
{

	private final Set<BinaryValue> adds = new HashSet<BinaryValue>();
	private final Set<BinaryValue> removes = new HashSet<BinaryValue>();

	/**
	 * Create an empty set update
	 */
	public SetUpdate()
	{
	}

	/**
	 * Add a member to the set
	 *
	 * @param value value to add
	 * @return this
	 */
	public SetUpdate add(byte[] value)
	{
		this.adds.add(BinaryValue.create(value));
		return this;
	}

	/**
	 * Remove an element from the set
	 *
	 * @param value value to remove
	 * @return this
	 */
	public SetUpdate remove(BinaryValue value)
	{
		this.removes.add(value);
		return this;
	}

	/**
	 * Get the set of additions
	 *
	 * @return accumulated set of additions
	 */
	public Set<BinaryValue> getAdds()
	{
		return adds;
	}

	/**
	 * Get the set of removals
	 *
	 * @return accumulated set of removals
	 */
	public Set<BinaryValue> getRemoves()
	{
		return removes;
	}

	@Override
	public SetOp getOp()
	{
		return new SetOp(adds, removes);
	}
}
