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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class BigIntIndexQuery extends SecondaryIndexQuery<BigInteger, BigIntIndexQuery.Response, BigIntIndexQuery>
{
    private final IndexConverter<BigInteger> converter;

    @Override
    protected IndexConverter<BigInteger> getConverter()
    {
        return converter;
    }
    
    protected BigIntIndexQuery(Init<BigInteger,?> builder)
    {
        super(builder);
        this.converter = new IndexConverter<BigInteger>()
        {
            @Override
            public BigInteger convert(BinaryValue input)
            {
                // Riak. US_ASCII string instead of integer
                return new BigInteger(input.toStringUtf8());
            }

            @Override
            public BinaryValue convert(BigInteger input)
            {
                return BinaryValue.createFromUtf8(input.toString());
            }
        };
    }
    
    @Override
    protected RiakFuture<Response, BigIntIndexQuery> executeAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
            executeCoreAsync(cluster);
        
        BigIntQueryFuture future = new BigIntQueryFuture(coreFuture);
        coreFuture.addListener(future);
        return future;
            
    }
    
    protected final class BigIntQueryFuture extends CoreFutureAdapter<Response, BigIntIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
    {
        public BigIntQueryFuture(RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(coreFuture);
        }

        @Override
        protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
        {
            return new Response(location, coreResponse, converter);
        }

        @Override
        protected BigIntIndexQuery convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
        {
            return BigIntIndexQuery.this;
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
    
    public static class Builder extends Init<BigInteger, Builder>
    {

        public Builder(Location location, String indexName, BigInteger start, BigInteger end)
        {
            super(location, indexName, start, end);
        }

        public Builder(Location location, String indexName, BigInteger match)
        {
            super(location, indexName, match);
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public BigIntIndexQuery build()
        {
            return new BigIntIndexQuery(this);
        }
    }
    
    public static class Response extends SecondaryIndexQuery.Response<BigInteger>
    {
        protected Response(Location queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<BigInteger> converter)
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

        public class Entry extends SecondaryIndexQuery.Response.Entry<BigInteger>
        {
            protected Entry(Location riakObjectLocation, BinaryValue indexKey, IndexConverter<BigInteger> converter)
            {
                super(riakObjectLocation, indexKey, converter);
            }

        }
    }
}
