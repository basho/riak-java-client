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
package com.basho.riak.client.core.query.indexes;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.nio.charset.Charset;

/**
 * {@code RiakIndex} implementation used to access a Riak {@code _int} Secondary Index using {@code Long} values.
 *
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation
 * of {@code RiakIndex} provides access to those bytes by converting to
 * and from {@code Long} values. Its type is {@link IndexType#INT}
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakIndexes
 * @see RiakObject#getIndexes()
 */
public class LongIntIndex extends RiakIndex<Long>
{
    private LongIntIndex(Name name)
    {
        super(name);
    }

    @Override
    protected BinaryValue convert(Long value)
    {
        // The Protocol Buffers API takes the bytes for the textual representation
        // of the number rather than an actual bytes for the number :/
        return BinaryValue.unsafeCreate(value.toString().getBytes(Charset.forName("UTF-8")));
    }

    @Override
    protected Long convert(BinaryValue value)
    {
        // The Protocol Buffers API returns the bytes for the textual representation
        // of the number rather than an actual bytes for the number :/
        return Long.valueOf(value.toString(Charset.forName("UTF-8")));
    }

    public static Name named(String name)
    {
        return new Name(name);
    }

    /**
     * Encapsulates the name and {@code IndexType} for a {@code LongIntIndex}
     */
    public static class Name extends RiakIndex.Name<LongIntIndex>
    {
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * @param name The name of this index.
         */
        Name(String name)
        {
            super(name, IndexType.INT);
        }

        @Override
        LongIntIndex createIndex()
        {
            return new LongIntIndex(this);
        }

    }
}
