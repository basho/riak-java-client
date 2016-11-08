/*
 * Copyright 2013-2016 Basho Technologies Inc
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
package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.operations.ts.CreateTableOperation;
import com.basho.riak.client.core.query.timeseries.TableDefinition;

/**
 * Time Series Create Command
 * Allows you to create a Riak Time Series table according to the provided definition.
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.6
 */
public class CreateTable extends AsIsRiakCommand<Void, String>
{
    private final Builder builder;

    private CreateTable(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected FutureOperation<Void, ?, String> buildCoreOperation() {
        return builder.buildOperation();
    }

    public static class Builder extends CreateTableOperation.AbstractBuilder<CreateTable, Builder>
    {
        /**
         * Creates a new Builder for the CreateTable command.
         * If any quantum information is present in the {@code tableDefinition}'s column descriptions,
         * it will be used automatically. If there is none present, please use
         * {@link CreateTable.Builder#withQuantum(int, java.util.concurrent.TimeUnit)} to set the quantum information.
         * @param tableDefinition The table definition to base this CreateTable command off of.
         */
        public Builder(TableDefinition tableDefinition)
        {
            super(tableDefinition);
        }

        public CreateTable build()
        {
            return new CreateTable(this);
        }
    }
}
