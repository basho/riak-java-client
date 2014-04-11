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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import java.util.ArrayList;
import java.util.List;

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
    protected RiakFuture<Response, IntIndexQuery> executeAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
            executeCoreAsync(cluster);
        
        IntQueryFuture future = new IntQueryFuture(coreFuture);
        coreFuture.addListener(future);
        return future;
            
    }

    protected final class IntQueryFuture extends CoreFutureAdapter<Response, IntIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
    {
        public IntQueryFuture(RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(coreFuture);
        }
        
        @Override
        protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
        {
            return new Response(location, coreResponse, converter);
        }

        @Override
        protected IntIndexQuery convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
        {
            return IntIndexQuery.this;
        }
    }
    
    protected static abstract class Init<S, T extends Init<S,T>> extends SecondaryIndexQuery.Init<S,T>
    {

        public Init(Location location, String indexName, S start, S end)
        {
            super(location, indexName + Type._INT, start, end);
        }

        public Init(Location location, String indexName, S match)
        {
            super(location, indexName + Type._INT, match);
        }
        
        @Override
        public T withRegexTermFilter(String filter)
        {
            throw new IllegalArgumentException("Cannot use term filter with _int query");
        }
    }

    public static class Builder extends Init<Long, Builder>
    {

        public Builder(Location location, String indexName, Long start, Long end)
        {
            super(location, indexName, start, end);
        }

        public Builder(Location location, String indexName, Long match)
        {
            super(location, indexName, match);
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
        protected Response(Location queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<Long> converter)
        {
            super(queryLocation, coreResponse, converter);
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
