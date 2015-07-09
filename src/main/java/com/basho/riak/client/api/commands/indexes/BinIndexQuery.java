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
import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery.IndexConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.DefaultCharset;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs a 2i query where the 2i index keys are strings.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * A RawIndexQuery is used when you are using strings for your 2i keys. The
 * parameters are provided as String objects. 
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * String key = "some_key";
 * BinIndexQuery q = new BinIndexQuery.Builder(ns, "my_index", key).build();
 * BinIndexQuery.Response resp = client.execute(q);}</pre>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class BinIndexQuery extends SecondaryIndexQuery<String, BinIndexQuery.Response, BinIndexQuery>
{
    private final Charset charset;
    private final IndexConverter<String> converter;

    protected  BinIndexQuery(Init<String,?> builder)
    {
        super(builder);
        this.charset = builder.charset;
        this.converter = new IndexConverter<String>() 
        {
            @Override
            public String convert(BinaryValue input)
            {
                return input.toString(charset);
            }

            @Override
            public BinaryValue convert(String input)
            {
                if (input == null )
                {
                    return null;
                }
                return BinaryValue.create(input, charset);
            }
        };
    }

    @Override 
    protected IndexConverter<String> getConverter()
    {
        return converter;
    }

    @Override
    protected RiakFuture<Response, BinIndexQuery> executeAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
            executeCoreAsync(cluster);
        
        BinQueryFuture future = new BinQueryFuture(coreFuture);
        coreFuture.addListener(future);
        return future;
    }

    protected final class BinQueryFuture extends CoreFutureAdapter<Response, BinIndexQuery, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
    {
        public BinQueryFuture(RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(coreFuture);
        }
        
        @Override
        protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
        {
            return new Response(namespace, coreResponse, converter);
        }

        @Override
        protected BinIndexQuery convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
        {
            return BinIndexQuery.this;
        }
    }
    
    protected static abstract class Init<S, T extends Init<S,T>> extends SecondaryIndexQuery.Init<S,T>
    {
        private Charset charset = DefaultCharset.get();

        public Init(Namespace namespace, String indexName, S start, S end)
        {
            super(namespace, indexName + Type._BIN, start, end);
        }

        public Init(Namespace namespace, String indexName, S match)
        {
            super(namespace, indexName + Type._BIN, match);
        }

        public Init(Namespace namespace, String indexName, byte[] coverContext)
        {
            super(namespace, indexName + Type._INT, coverContext);
        }

        T withCharacterSet(Charset charset)
        {
            this.charset = charset;
            return self();
        }

    }

    /**
     * Builder used to construct a BinIndexQuery.
     */
    public static class Builder extends Init<String, Builder>
    {
        /**
         * Construct a Builder for a BinIndexQuery with a cover context.
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
         * Construct a Builder for a BinIndexQuery with a range.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension. 
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The index name in Riak to query.
         * @param start The start of the 2i range.
         * @param end The end of the 2i range.
         */
        public Builder(Namespace namespace, String indexName, String start, String end)
        {
            super(namespace, indexName, start, end);
        }

        /**
         * Construct a Builder for a BinIndexQuery with a single 2i key.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension. 
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param match the 2i key.
         */
        public Builder(Namespace namespace, String indexName, String match)
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
         * @return a new BinIndexQuery
         */
        public BinIndexQuery build()
        {
            return new BinIndexQuery(this);
        }

    }
    
    public static class Response extends SecondaryIndexQuery.Response<String>
    {
        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<String> converter)
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

        public class Entry extends SecondaryIndexQuery.Response.Entry<String>
        {
            protected Entry(Location riakObjectLocation, BinaryValue indexKey, IndexConverter<String> converter)
            {
                super(riakObjectLocation, indexKey, converter);
            }

        }
    }
}
