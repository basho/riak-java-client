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

package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.YzGetSchemaOperation;

/**
 * Command used to fetch a search schema from Riak.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchSchema extends AsIsRiakCommand<YzGetSchemaOperation.Response, String>
{
    private final String schema;

    FetchSchema(Builder builder)
    {
        this.schema = builder.schema;
    }

    protected YzGetSchemaOperation buildCoreOperation()
    {
        return new YzGetSchemaOperation.Builder(schema).build();
    }

    /**
     * Builder for a FetchSchema command.
     */
    public static class Builder
    {
        private final String schema;

        /**
         * Construct a Builder for a FetchSchema command.
         *
         * @param schema The name of the schema to fetch.
         */
        public Builder(String schema)
        {
            this.schema = schema;
        }

        /**
         * Construct the FetchSchema command.
         * @return the new FetchSchema command.
         */
        public FetchSchema build()
        {
            return new FetchSchema(this);
        }
    }
}
