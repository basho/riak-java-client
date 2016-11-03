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

import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.indexes.IndexNames;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.DefaultCharset;

import java.nio.charset.Charset;

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
 *
 * <p>
 * You can also stream the results back before the operation is fully complete.
 * This reduces the time between executing the operation and seeing a result,
 * and reduces overall memory usage if the iterator is consumed quickly enough.
 * The result iterable can only be iterated once though.
 * If the thread is interrupted while the iterator is polling for more results,
 * a {@link RuntimeException} will be thrown.
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * String key = "some_key";
 * BinIndexQuery q = new BinIndexQuery.Builder(ns, "my_index", key).build();
 * RiakFuture<BinIndexQuery.StreamingResponse, BinIndexQuery> streamingFuture =
 *     client.executeAsyncStreaming(q, 200);
 * BinIndexQuery.StreamingResponse streamingResponse = streamingFuture.get();
 *
 * for (BinIndexQuery.Response.Entry e : streamingResponse)
 * {
 *     System.out.println(e.getRiakObjectLocation().getKey().toString());
 * }
 * // Wait for the command to fully finish.
 * streamingFuture.await();
 * // The StreamingResponse will also contain the continuation, if the operation returned one.
 * streamingResponse.getContinuation(); }</pre>
 * </p>
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0
 */
public class BinIndexQuery extends SecondaryIndexQuery<String, BinIndexQuery.Response, BinIndexQuery>
{
    private final Charset charset;
    private final IndexConverter<String> converter;

    protected BinIndexQuery(Init<String, ?> builder)
    {
        super(builder, Response::new, Response::new);
        this.charset = builder.charset;
        this.converter = new StringIndexConverter();
    }

    @Override
    protected IndexConverter<String> getConverter()
    {
        return converter;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        BinIndexQuery that = (BinIndexQuery) o;

        if (!charset.equals(that.charset))
        {
            return false;
        }
        if (!converter.equals(that.converter))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + charset.hashCode();
        result = 31 * result + converter.hashCode();
        return result;
    }

    protected static abstract class Init<S, T extends Init<S, T>> extends SecondaryIndexQuery.Init<S, T>
    {
        private Charset charset = DefaultCharset.get();

        public Init(Namespace namespace, String indexName, S start, S end)
        {
            super(namespace, generateIndexName(indexName), start, end);
        }

        public Init(Namespace namespace, String indexName, S match)
        {
            super(namespace, generateIndexName(indexName), match);
        }

        public Init(Namespace namespace, String indexName, byte[] coverContext)
        {
            super(namespace, generateIndexName(indexName), coverContext);
        }

        private static String generateIndexName(String baseIndexName)
        {
            if (IndexNames.BUCKET.equalsIgnoreCase(baseIndexName) ||
               IndexNames.KEY.equalsIgnoreCase(baseIndexName))
            {
                return baseIndexName;
            }

            return baseIndexName + Type._BIN;
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
         *
         * @param namespace The namespace in Riak to query.
         * @param indexName The index name in Riak to query.
         * @param start     The start of the 2i range.
         * @param end       The end of the 2i range.
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
         *
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param match     the 2i key.
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
         *
         * @return a new BinIndexQuery
         */
        public BinIndexQuery build()
        {
            return new BinIndexQuery(this);
        }
    }

    public static class Response extends SecondaryIndexQuery.Response<String, SecondaryIndexQuery.Response.Entry<String>>
    {

        protected Response(Namespace queryLocation, IndexConverter<String> converter, ChunkedResponseIterator<Entry, ?, ?> chunkedResponseIterator) {
            super(queryLocation, converter, chunkedResponseIterator);
        }

        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<String> converter) {
            super(queryLocation, coreResponse, converter);
        }
    }

    private class StringIndexConverter implements IndexConverter<String>
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return 0;
        }
    }
}
