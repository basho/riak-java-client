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

/**
 * Enum that encapsulates the suffix used to determine and index type in Riak.
 * <p>
 * There are two types of Seconrady Indexes (2i) in Riak; "Integer" and 
 * "Binary". The current server API distinguishes between them via a 
 * suffix ({@code "_int"} and {@code "_bin"} respectively).
 * <p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakIndex
 */
public enum IndexType
{
    /**
     * Encapsulates the {@code "_int"} suffix for Riak index names.
     */
    INT("_int"), 
    /**
     * Encapsulates the {@code "_bin"} suffix for Riak index names.
     */
    BIN("_bin");
    
    private final String suffix;
    
    private IndexType(String suffix)
    {
        this.suffix = suffix;
    }
    
    /**
     * Returns the suffix for this type.
     * @return a {@code String} containing the suffix for this index type.
     */
    public String suffix()
    {
        return suffix;
    }
    
    /**
     * Returns the index type from its fully qualified name <p> There are two
     * types of Seconrady Indexes (2i) in Riak; "Integer" and "Binary". The
     * current server API distinguishes between them via a suffix ({@code _int}
     * and {@code _bin} respectively). This method takes a "fully qualified" 2i
     * name (e.g. "my_index_int") and returns an enum that represents the type.
     *
     * @param fullname a "fully qualified" 2i name ending with the suffix "_int"
     * or "_bin"
     * @return the {@link IndexType}
     * @throws IllegalArgumentException if the supplied index name does not have
     * a valid suffix.
     */
    public static IndexType typeFromFullname(String fullname)
    {
        int i = fullname.lastIndexOf('_');
        if (i != -1)
        {
            String suffix = fullname.substring(i);
            for (IndexType t : IndexType.values())
            {
                if (t.suffix().equalsIgnoreCase(suffix))
                {
                    return t;
                }
            }
        }

        throw new IllegalArgumentException("Indexname does not end with valid suffix");
    }
}
