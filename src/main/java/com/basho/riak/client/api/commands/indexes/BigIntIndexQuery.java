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

package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

import java.math.BigInteger;

/**
 * Performs a 2i query where the 2i index keys are numeric.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * A BigIntIndexQuery is used when you are using integers for your 2i keys. The
 * parameters are provided as BigInteger values. Use this query if your
 * 2i key values exceed that
 * which can be stored in a (64 bit) long,
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * BigInteger key = new BigInteger(someReallyLongNumber);
 * BigIntIndexQuery q = new BigIntIndexQuery.Builder(ns, "my_index", key).build();
 * BigIntIndexQuery.Response resp = client.execute(q);}</pre>
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
 * BigInteger key = new BigInteger(someReallyLongNumber);
 * BigIntIndexQuery q = new BigIntIndexQuery.Builder(ns, "my_index", key).build();
 * RiakFuture<BigIntIndexQuery.StreamingResponse, BigIntIndexQuery> streamingFuture =
 *     client.executeAsyncStreaming(q, 200);
 * BigIntIndexQuery.StreamingResponse streamingResponse = streamingFuture.get();
 *
 * for (BigIntIndexQuery.Response.Entry e : streamingResponse)
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
public class BigIntIndexQuery
        extends SecondaryIndexQuery<BigInteger,BigIntIndexQuery.Response, BigIntIndexQuery>
{
    private final IndexConverter<BigInteger> converter;

    @Override
    protected IndexConverter<BigInteger> getConverter()
    {
        return converter;
    }

    protected BigIntIndexQuery(Init<BigInteger,?> builder)
    {
        super(builder, Response::new, Response::new);
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
                if (input == null)
                {
                    return null;
                }
                return BinaryValue.createFromUtf8(input.toString());
            }
        };
    }

    protected static abstract class Init<S, T extends Init<S,T>> extends SecondaryIndexQuery.Init<S,T>
    {
        public Init(Namespace namespace, String indexName, S start, S end)
        {
            super(namespace, indexName + Type._INT, start, end);
        }

        public Init(Namespace namespace, String indexName, S match)
        {
            super(namespace, indexName + Type._INT, match);
        }

        public Init(Namespace namespace, String indexName, byte[] coverContext)
        {
            super(namespace, indexName + Type._INT, coverContext);
        }

        @Override
        public T withRegexTermFilter(String filter)
        {
            throw new IllegalArgumentException("Cannot use term filter with _int query");
        }
    }

    /**
     * Builder used to construct a BigIntIndexQuery.
     */
    public static class Builder extends Init<BigInteger, Builder>
    {
        /**
         * Construct a Builder for a BigIntIndexQuery with a cover context.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension.
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param coverContext cover context.
         */
        public Builder(Namespace namespace, String indexName,  byte[] coverContext)
        {
            super(namespace, indexName, coverContext);
        }

        /**
         * Construct a Builder for a BigIntIndexQuery with a range.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension.
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param start the start of the 2i range.
         * @param end the end of the 2i range.
         */
        public Builder(Namespace namespace, String indexName, BigInteger start, BigInteger end)
        {
            super(namespace, indexName, start, end);
        }

        /**
         * Construct a Builder for a BigIntIndexQuery with a single 2i key.
         * <p>
         * Note that your index name should not include the Riak {@literal _int} or
         * {@literal _bin} extension.
         * <p>
         * @param namespace The namespace in Riak to query.
         * @param indexName The name of the index in Riak.
         * @param match the 2i key.
         */
        public Builder(Namespace namespace, String indexName, BigInteger match)
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
         * @return a new BigIntIndexQuery.
         */
        public BigIntIndexQuery build()
        {
            return new BigIntIndexQuery(this);
        }
    }

    public static class Response extends SecondaryIndexQuery.Response<BigInteger, SecondaryIndexQuery.Response.Entry<BigInteger>>
    {
        Response(Namespace queryLocation, IndexConverter<BigInteger> converter, int timeout, StreamingRiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(queryLocation, converter, timeout, coreFuture);
        }

        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<BigInteger> converter)
        {
            super(queryLocation, coreResponse, converter);
        }
    }
}
