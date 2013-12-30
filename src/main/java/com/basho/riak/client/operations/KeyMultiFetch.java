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

import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

class KeyMultiFetch<T> extends MultiFetch<T>
{

    private final Converter<T> converter;
    private final ArrayList<Location> keys;

    KeyMultiFetch(Converter<T> converter, Location... keys)
    {
        this.converter = converter;
        this.keys = new ArrayList<Location>(Arrays.asList(keys));
    }

    KeyMultiFetch(Converter<T> converter, Iterable<Location> keys)
    {
        this.converter = converter;
        this.keys = new ArrayList<Location>();
        Iterator<Location> itr = keys.iterator();
        while (itr.hasNext())
        {
            this.keys.add(itr.next());
        }
    }

    @Override
    Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        List<FetchValue.Response> values = new ArrayList<FetchValue.Response>();
        for (Location key : keys)
        {
            values.add(new FetchValue.Builder<T>(key).withConverter(converter).build().execute(cluster));
        }

        return new Response(values);

    }
}
