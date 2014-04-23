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

/**
 * A Riak datatype context. This is returned by a fetch, when requested, of
 * any Riak datatype and should be returned when performing a delete on a
 * {@link com.basho.riak.client.query.crdt.types.RiakSet}
 * or a {@link com.basho.riak.client.query.crdt.types.RiakMap}.
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class Context
{

	private final byte[] bytes;

	public Context(byte[] bytes)
	{
		this.bytes = bytes;
	}

	public byte[] getBytes()
	{
		return bytes;
	}
}
