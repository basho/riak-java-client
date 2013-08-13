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

import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.nio.charset.Charset;

/**
 * An index using Strings
 * <p>
 * Data in Riak including secondary indexes is stored as bytes. This implementation 
 * of {@code RiakIndex} provides access to those bytes by converting to 
 * and from {@code String} values.  
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
    protected ByteArrayWrapper convert(String value)
    {
        return ByteArrayWrapper.unsafeCreate(value.getBytes(charset));
    }

    @Override
    protected String convert(ByteArrayWrapper value)
    {
        return value.toString(charset);
    }
    
    public static class Name extends RiakIndex.Name<StringBinIndex>
    {
        private final Charset charset;
        
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * The default character set is used for encoding the values.
         * @param name The name of this index.
         */
        public Name(String name)
        {
            this(name, Charset.defaultCharset());
        }
        /**
         * Constructs a RiakIndex.Name to be used with {@link RiakIndexes}
         * The supplied character set is used for encoding the values.
         * @param name The name of this index.
         * @param charset The character set to use for encoding values
         */
        public Name(String name, Charset charset)
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
