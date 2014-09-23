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
package com.basho.riak.client.core.query.crdt.types;

/**
 * Representation of the Riak counter datatype.
 * <p>
 * This is an immutable value returned when querying Riak for a counter datatype.
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class RiakCounter extends RiakDatatype
{

    private long value = 0;

    public RiakCounter(long value)
    {
        this.value = value;
    }

    /**
     * Get this RiakCounter as a {@link Long} value.
     * @return a Long value for this RiakCounter.
     */
	@Override
    public Long view()
    {
        return value;
    }

}
