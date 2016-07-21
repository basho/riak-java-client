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

package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs a 2i query where the 2i index keys are numeric.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * A IntIndexQuery is used when you are using integers for your 2i keys. The
 * parameters are provided as long values.
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * long key = 1234L;
 * IntIndexQuery q = new IntIndexQuery.Builder(ns, "my_index", key).build();
 * IntIndexQuery.Response resp = client.execute(q);}</pre>
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
                if (input == null)
                {
                    return null;
                }
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
            return new Response(namespace, coreResponse, converter);
        }

        @Override
        protected IntIndexQuery convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
        {
            return IntIndexQuery.this;
        }
    }
    
    protected static abstract class Init<S, T extends Init<S,T>> extends SecondaryIndexQuery.Init<S,T>
    {

        public Init(Namespace namespace, String indexName, S start, S end)
        {
            super(namespace, indexName + Type._INT, start, end);
        }

        public Init(Namespace namespace, String indexName, byte[] coverContext)
        {
            super(namespace, indexName + Type._INT, coverContext);
        }

        public Init(Namespace namespace, String indexName, S match)
        {
            super(namespace, indexName + Type._INT, match);
        }
        
        @Override
        public T withRegexTermFilter(String filter)
        {
            throw new IllegalArgumentException("Cannot use term filter with _int query");
        }
    }

    /**
     * Builder used to construct a IntIndexQuery.
     */
    public static class Builder extends Init<Long, Builder>
    {

        /**
         * Construct a Builder for a IntIndexQuery with a cover context.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension.
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param coverContext cover context.
         */
        public Builder(Namespace namespace, String indexName, byte[] coverContext)
        {
            super(namespace, indexName, coverContext);
        }

        /**
         * Construct a Builder for a IntIndexQuery with a range.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension. 
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param start the start of the 2i range.
         * @param end the end of the 2i range.
         */
        public Builder(Namespace namespace, String indexName, Long start, Long end)
        {
            super(namespace, indexName, start, end);
        }

        /**
         * Construct a Builder for a IntIndexQuery with a single 2i key.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension. 
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param match the 2i key.
         */
        public Builder(Namespace namespace, String indexName, Long match)
        {
            super(namespace, indexName, match);
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Construct the query.
         * @return a new IntIndexQuery
         */
        public IntIndexQuery build()
        {
            return new IntIndexQuery(this);
        }
    }
    
    public static class Response extends SecondaryIndexQuery.Response<Long>
    {
        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<Long> converter)
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
