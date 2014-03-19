/*
 * Copyright 2013 Basho Technologies Inc.
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

import com.basho.riak.client.util.BinaryValue;

/**
 * {@code RiakIndex} implementation used to access a Riak {@code _int} or {@code _bin} 
 * Secondary Index using {@code BinaryValue} ({@code byte[]}) values.
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation 
 * of {@code RiakIndex} provides direct access to those bytes. 
 * </p>
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RawIndex extends RiakIndex<BinaryValue>
{
    private RawIndex(Name name)
    {
        super(name);
    }
    
    @Override
    protected BinaryValue convert(BinaryValue value)
    {
        return value;
    }

    public static Name named(String name, IndexType type)
    {
        return new Name(name, type);
    }
    
    /**
     * Encapsulates the name and {@code IndexType} for a {@code RawIndex}
     */
    public static class Name extends RiakIndex.Name<RawIndex>
    {
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * @param name The name of this index.
         * @param type The IndexType of this index
         */
        Name(String name, IndexType type)
        {
            super(name, type);
        }
        
        @Override
        RawIndex createIndex()
        {
            return new RawIndex(this);
        }
        
    }
    
}
