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
package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.operations.FetchValue.fetch;

class IndexMultiFetch extends MultiFetch
{

    private final FetchIndex index;

    IndexMultiFetch(FetchIndex index)
    {
        this.index = index;
    }

    @Override
    Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        ArrayList<FetchValue.Response> values = new ArrayList<FetchValue.Response>();

        FetchIndex.Response result;
        do
        {
            result = index.execute(cluster);
            for (FetchIndex.IndexEntry entry : result)
            {
                Key key = entry.getKey();
                values.add(fetch(key).execute(cluster));
            }

            if (result.hasContinuation())
            {
                index.withContinuation(result.getContinuation());
            }
        }
        while (result.hasContinuation());

        return new Response(values);
    }
}
