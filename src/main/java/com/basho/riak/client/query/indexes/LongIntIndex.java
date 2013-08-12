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
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.nio.charset.Charset;

/**
 * An index with long values
 * 
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation 
 * of {@code RiakIndex} provides access to those bytes by converting to 
 * and from {@code Long} values.  
 * </p>
 * <p>
 * A static factory method {@link LongIntIndex#named(java.lang.String) } is provided to
 * create instances of this index and provide the appropriate {@link IndexType#INT} type.
 * A fluent interface is then provided for adding values:
 * <pre>
 * {@code
 * LongIntIndex index = LongIntIndex.named("my_index")
 *                                  .add(123456L)
 *                                  .add(234567L);
 * riakObject.getIndexes().add(index);
 * }
 * </pre>
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakIndexes
 * @see RiakObject#getIndexes() 
 */
public class LongIntIndex extends RiakIndex<Long>
{
    private LongIntIndex(String name)
    {
        super(name, IndexType.INT);
    }

    /**
     * Static factory method for creating a LongIntIndex
     * @param name the name for this index
     * @return a LongIntIndex with the provided name and {@link IndexType#INT} type.
     */
    public static LongIntIndex named(String name)
    {
        return new LongIntIndex(name);
    }
    
    @Override
    protected ByteArrayWrapper convert(Long value)
    {
        // The Protocol Buffers API takes the bytes for the textual representation
        // of the number rather than an actual bytes for the number :/ 
        return ByteArrayWrapper.unsafeCreate(value.toString().getBytes(Charset.forName("UTF-8")));
    }

    @Override
    protected Long convert(ByteArrayWrapper value)
    {
        // The Protocol Buffers API returns the bytes for the textual representation
        // of the number rather than an actual bytes for the number :/ 
        return Long.valueOf(value.toString(Charset.forName("UTF-8")));
    }
}
