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
 *  <p>
 * A static factory method {@link RawIndex#named(java.lang.String, com.basho.riak.client.query.indexes.IndexType) } 
 * is provided to create instances of this index. 
 * </p>
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
     * Static factory method that returns a RawIndex 
     * <p>
     * The appropriate {@link IndexType} must be provided.
     * <p>
     * @param name the name for this index
     * @param type the type for this index
     * @return a {@code RawIndex} with the provided name and index type. 
     */
    public static RawIndex named(String name, IndexType type)
    {
        return new Name(name, type).createIndex();
    }
    
    public static class Name extends RiakIndex.Name<ByteArrayWrapper>
    {
        public Name(String name, IndexType type)
        {
            super(name, type);
        }
        
        @Override
        public RawIndex createIndex()
        {
            return new RawIndex(this);
        }
        
    }
    
}
