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
package com.basho.riak.client.core.query.search;

import java.util.Objects;

/**
 * Represents a Yokozuna Index.
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YokozunaIndex
{
    private final String name;
    private final String schema;
    private volatile Integer nVal;

    /**
     * Constructs a Yokozuna index without naming the schema.
     * <p>
     * Due to an implementation detail on the Riak side, the index name is restricted
     * to US-ASCII characters. The supplied String is converted to bytes
     * using the UTF-8 Charset.
     *
     * @param name The name for this index.
     */
    public YokozunaIndex(String name)
    {
        this(name, null);
    }

    /**
     * Constructs a Yokozuna index.
     * Due to an implementation detail on the Riak side, the index name is restricted
     * to US-ASCII characters. The supplied String is converted to bytes
     * using the UTF-8 Charset.
     *
     * @param name   The name of the index.
     * @param schema The name of a schema
     */
    public YokozunaIndex(String name, String schema)
    {
        if (null == name || name.length() == 0)
        {
            throw new IllegalArgumentException("Index name can not be null or zero length");
        }

        this.name = name;
        this.schema = schema;
    }

    /**
     * Set the nVal.
     *
     * @param nVal the number of replicas.
     * @return a reference to this object.
     */
    public YokozunaIndex withNVal(int nVal)
    {
        if (nVal < 1)
        {
            throw new IllegalArgumentException("nVal must be >= 1");
        }

        this.nVal = nVal;
        return this;
    }

    /**
     * Returns the name of this index as a UTF-8.
     *
     * @return The name of the index as UTF-8 encoded String.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Returns the schema name for this index.
     *
     * @return The schema name.
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     * Determine if an nVal value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasNVal()
    {
        return nVal != null;
    }

    /**
     * Get the nVal.
     *
     * @return the nVal value or null if not set.
     */
    public Integer getNVal()
    {
        return nVal;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }

        if (!(other instanceof YokozunaIndex))
        {
            return false;
        }

        YokozunaIndex otherYokozunaIndex = (YokozunaIndex) other;

        return Objects.equals(name, otherYokozunaIndex.name) &&
               Objects.equals(schema, otherYokozunaIndex.schema) &&
               Objects.equals(nVal, otherYokozunaIndex.nVal);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, schema, nVal);
    }
}
