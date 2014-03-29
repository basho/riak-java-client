/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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
 */
public class RawIndexQuery extends SecondaryIndexQuery<BinaryValue, RawIndexQuery.Response, RawIndexQuery>
{
    private final IndexConverter<BinaryValue> converter;
    
    protected RawIndexQuery(Init<BinaryValue,?> builder)
    {
        super(builder);
        this.converter = new IndexConverter<BinaryValue>()
        {
            @Override
            public BinaryValue convert(BinaryValue input)
            {
                return input;
            }
        };
    }
    
    @Override
    protected IndexConverter<BinaryValue> getConverter()
    {
        return converter;
    }

    @Override
    protected Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        RiakFuture<Response, RawIndexQuery> future = 
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
    protected RiakFuture<Response, RawIndexQuery> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
            executeCoreAsync(cluster);
        
        RawQueryFuture future = new RawQueryFuture(coreFuture);
        coreFuture.addListener(future);
        return future;
    }

    protected final class RawQueryFuture extends CoreFutureAdapter<Response, RawIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
    {
        public RawQueryFuture(RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(coreFuture);
        }
        
        @Override
        protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
        {
            return new Response(coreResponse, converter);
        }

        @Override
        protected FailureInfo<RawIndexQuery> convertFailureInfo(FailureInfo<SecondaryIndexQueryOperation.Query> coreQueryInfo)
        {
            return new FailureInfo<RawIndexQuery>(coreQueryInfo.getCause(), RawIndexQuery.this);
        }
        
    }
    
    public static class Builder extends SecondaryIndexQuery.Init<BinaryValue, Builder>
    {

        public Builder(Location location, String indexName, Type type, BinaryValue start, BinaryValue end)
        {
            super(location, indexName + type, start, end);
        }

        public Builder(Location location, String indexName, Type type, BinaryValue match)
        {
            super(location, indexName + type, match);
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public RawIndexQuery build()
        {
            return new RawIndexQuery(this);
        }
    }
    
    public static class Response extends SecondaryIndexQuery.Response<BinaryValue>
    {
        protected Response(SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<BinaryValue> converter)
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

        public class Entry extends SecondaryIndexQuery.Response.Entry<BinaryValue>
        {
            protected Entry(Location riakObjectLocation, BinaryValue indexKey, IndexConverter<BinaryValue> converter)
            {
                super(riakObjectLocation, indexKey, converter);
            }

        }
    }
    
}
