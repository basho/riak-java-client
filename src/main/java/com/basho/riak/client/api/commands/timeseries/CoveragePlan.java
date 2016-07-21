/*
 * Copyright 2013-2015 Basho Technologies Inc
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

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.CoveragePlanOperation;
import com.basho.riak.client.core.query.timeseries.CoveragePlanResult;

public class CoveragePlan extends RiakCommand<CoveragePlanResult, String>
{
    private final CoveragePlanOperation operation;

    private CoveragePlan(Builder builder)
    {
        this.operation = builder.buildOperation();
    }

    @Override
    protected RiakFuture<CoveragePlanResult, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<CoveragePlanResult, String> future = cluster.execute(operation);
        return future;
    }

    public static class Builder extends CoveragePlanOperation.AbstractBuilder<CoveragePlan>
    {
        public Builder(String tableName, String query)
        {
            super(tableName, query);
        }

        @Override
        public CoveragePlan build()
        {
            return new CoveragePlan(this);
        }

        public static Builder create(String tableName, String query)
        {
            return new Builder(tableName, query);
        }
    }
}