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

/**
 *
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public final class IntIndex extends RiakIndex<Number>
{

    private static final String SUFFIX = "_int";

    /**
     * @param name
     * @param index
     */
    private IntIndex(String name)
    {
        super(name);
    }

    /**
     * Factory method, create a new IntIndex
     *
     * @param name the index name (**WITHOUT** any Riak specific suffix! e.g.
     * use "age" not "age_int")
     * @return an IntIndex named <code>name</code>
     */
    public static IntIndex named(String name)
    {
        return new IntIndex(name);
    }

    @Override
    protected String getSuffix()
    {
        return SUFFIX;
    }
}
