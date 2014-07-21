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
package com.basho.riak.client.core.query.indexes;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.nio.charset.Charset;

/**
 * {@code RiakIndex} implementation used to access a Riak {@code _bin} Secondary Index using {@code String} values.
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation 
 * of {@code RiakIndex} provides access to those bytes by converting to 
 * and from {@code String} values.  Its type is {@link IndexType#BIN} 
 * </p>
 * <h6>Riak 2i _bin indexes and sorting</h6>
 * <p>
 * One of the key features of 2i is the ability to do range queries. As previously 
 * noted the values are stored in Riak as bytes. Comparison is done byte-by-byte. UTF-8
 * lends itself well to this as its byte ordering is the same as its lexical ordering.
 * </p> 
 * <p>
 * If you are using a character asSet whose byte ordering differs from its lexical ordering,
 * range queries will be affected.
 * </p>
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakIndexes
 * @see RiakObject
 */
public class StringBinIndex extends RiakIndex<String>
{
    private final Charset charset;
    
    private StringBinIndex(Name name)
    {
        super(name);
        this.charset = name.charset;
    }
    
    @Override
    protected BinaryValue convert(String value)
    {
        return BinaryValue.unsafeCreate(value.getBytes(charset));
    }

    @Override
    protected String convert(BinaryValue value)
    {
        return value.toString(charset);
    }
    
    public static RiakIndex.Name<StringBinIndex> named(String name)
    {
        return named(name, Charset.defaultCharset());
    }
    
    public static Name named(String name, Charset charset)
    {
        return new Name(name, charset);
    }
    
    /**
     * Encapsulates the name, character asSet, and {@code IndexType} for a {@code StringBinIndex}
     */
    public static class Name extends RiakIndex.Name<StringBinIndex>
    {
        private final Charset charset;
        
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * The default character asSet is used for encoding the values.
         * @param name The name of this index.
         */
        Name(String name)
        {
            this(name, Charset.defaultCharset());
        }
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * The supplied character asSet is used for encoding the values.
         * @param name The name of this index.
         * @param charset The character asSet to use for encoding values
         */
        Name(String name, Charset charset)
        {
            super(name, IndexType.BIN);
            this.charset = charset;
        }

        @Override
        StringBinIndex createIndex()
        {
            return new StringBinIndex(this);
        }
        
        

    }
    
}
