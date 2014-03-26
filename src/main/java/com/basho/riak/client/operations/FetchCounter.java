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

package com.basho.riak.client.operations;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakCounter;
import com.basho.riak.client.query.crdt.types.RiakDatatype;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchCounter extends FetchDatatype<RiakCounter>
{
	private FetchCounter(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakCounter extractDatatype(RiakDatatype element)
	{
		return element.getAsCounter();
	}

	public static class Builder extends FetchDatatype.Builder<Builder>
	{

		public Builder(Location location)
		{
			super(location);
		}

		@Override
		protected Builder self()
		{
			return this;
		}

		public FetchCounter build()
		{
			return new FetchCounter(this);
		}
	}
}
