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

import com.basho.riak.client.util.ByteArrayWrapper;

/**
 * An index with values as raw bytes.
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation 
 * of {@code RiakIndex} provides direct access to those bytes.
 * </p>
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RawIndex extends RiakIndex<ByteArrayWrapper>
{
    private RawIndex(Name name)
    {
        super(name);
    }
    
    @Override
    protected ByteArrayWrapper convert(ByteArrayWrapper value)
    {
        return value;
    }

    /**
     * Encapsulates the name for a RawIndex
     */
    public static class Name extends RiakIndex.Name<RawIndex>
    {
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * @param name The name of this index.
         * @param type The IndexType of this index
         */
        public Name(String name, IndexType type)
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
