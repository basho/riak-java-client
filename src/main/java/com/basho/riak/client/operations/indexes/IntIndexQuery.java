/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.operations.indexes;

import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class IntIndexQuery extends SecondaryIndexQuery<Long, IntIndexQuery.Response, IntIndexQuery>
{
    private final IndexConverter<Long> converter;

    @Override
    protected IndexConverter<Long> getConverter()
    {
        return converter;
    }

    protected IntIndexQuery(Init<Long,?> builder)
    {
        super(builder);
        this.converter = new IndexConverter<Long>()
        {
            @Override
            public Long convert(BinaryValue input)
            {
                // Riak. US_ASCII string instead of integer
                return Long.valueOf(input.toStringUtf8());
            }

            @Override
            public BinaryValue convert(Long input)
            {
                // Riak. US_ASCII string instead of integer
                return BinaryValue.createFromUtf8(String.valueOf(input));
            }

        };
    }

    @Override
    protected Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        RiakFuture<Response, IntIndexQuery> future = 
            doExecuteAsync(cluster);
        
        future.await();
        
        if (future.isSuccess())
        {
            return future.get();
        }
        else
        {
            throw new ExecutionException(future.cause().getCause());
        }
    }

    @Override
    protected RiakFuture<Response, IntIndexQuery> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
            executeCoreAsync(cluster);
        
        CoreFutureAdapter<Response, IntIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> future =
            new CoreFutureAdapter<Response, IntIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>(coreFuture)
            {
                @Override
                protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
                {
                    return new Response(coreResponse, converter);
                }

                @Override
                protected FailureInfo<IntIndexQuery> convertFailureInfo(FailureInfo<SecondaryIndexQueryOperation.Query> coreQueryInfo)
                {
                    return new FailureInfo<IntIndexQuery>(coreQueryInfo.getCause(), IntIndexQuery.this);
                }
            };
        coreFuture.addListener(future);
        return future;
            
    }

//    protected static abstract class Init<Long, T extends Init<Long,T>> extends SecondaryIndexQuery.Init<Long,T>
//    {
//
//        public Init(Location location, String indexName, Long start, Long end)
//        {
//            super(location, indexName + "_int", start, end);
//        }
//
//        public Init(Location location, String indexName, Long match)
//        {
//            super(location, indexName + "_int", match);
//        }
//    }

    public static class Builder extends SecondaryIndexQuery.Init<Long, Builder>
    {

        public Builder(Location location, String indexName, Long start, Long end)
        {
            super(location, indexName + Type._INT, start, end);
        }

        public Builder(Location location, String indexName, Long match)
        {
            super(location, indexName + Type._INT, match);
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public IntIndexQuery build()
        {
            return new IntIndexQuery(this);
        }
    }
    
    public static class Response extends SecondaryIndexQuery.Response<Long>
    {
        protected Response(SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<Long> converter)
        {
            super(coreResponse, converter);
        }
        
        @Override
        public List<Entry> getEntries()
        {
            List<Entry> convertedList = new ArrayList<Entry>();
            for (SecondaryIndexQueryOperation.Response.Entry e : coreResponse.getEntryList())
            {
                Location loc = getLocationFromCoreEntry(e);
                Entry ce = new Entry(loc, e.getIndexKey(), converter);
                convertedList.add(ce);
            }
            return convertedList;
        }

        public class Entry extends SecondaryIndexQuery.Response.Entry<Long>
        {
            protected Entry(Location riakObjectLocation, BinaryValue indexKey, IndexConverter<Long> converter)
            {
                super(riakObjectLocation, indexKey, converter);
            }

        }
    }
}
