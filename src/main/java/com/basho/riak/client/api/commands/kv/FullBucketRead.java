/*
 * Copyright 2015 Basho Technologies Inc.
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
package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery;
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.indexes.IndexNames;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.List;

/**
 * Command used to retrieve all values from  Riak bucket.
 * <p>
 * Command might be executed with or without
 * {@link com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry}.
 * If coverage entry/context is provided, the only primary writes that
 * related to the coverage context will be returned, otherwise all data will be returned.
 * All execution options may be used in conjunction with a coverage context.
 *
 * Unlike 2i queries, FullBucketRead may return values/objects as part
 * of the response and, as a result, it has better performance.
 * To activate this option you need to set the
 * {@link com.basho.riak.client.api.commands.kv.FullBucketRead.Builder#returnBody} parameter.
 *
 * Note that this command must not be used without coverage context
 * for querying buckets that contain a big amount of data.
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @see CoveragePlan
 */
public class FullBucketRead extends SecondaryIndexQuery<BinaryValue, FullBucketRead.Response, FullBucketRead>
{
    private final IndexConverter<BinaryValue> converter;

    protected FullBucketRead(Builder builder)
    {
        super(builder.get2iBuilder(), Response::new, Response::new);
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

    /**
     * Builder used to construct a FullBucketRead command.
     */
    public static class Builder
    {
        private static class BuilderFullBucketRead2i
                extends SecondaryIndexQuery.Init<BinaryValue, BuilderFullBucketRead2i>
        {
            public BuilderFullBucketRead2i(Namespace namespace)
            {
                super(namespace, IndexNames.BUCKET, namespace.getBucketName());
            }

            public BuilderFullBucketRead2i(Namespace namespace, byte[] coverageContext)
            {
                super(namespace, IndexNames.BUCKET, coverageContext);
            }

            @Override
            protected BuilderFullBucketRead2i self()
            {
                return this;
            }

            @Override
            public BuilderFullBucketRead2i withReturnBody(boolean returnBody)
            {
                return super.withReturnBody(returnBody);
            }
        }
        private final BuilderFullBucketRead2i builder2i;

        /**
         * Construct a Builder for a FullBucketRead with a cover context.
         *
         * <p>
         * Note that this command mustn't be used without coverage context for querying buckets
         * that contain a big amount of data.
         * <p>
         */
        public Builder(Namespace namespace)
        {
            builder2i = new BuilderFullBucketRead2i(namespace);
        }

        /**
         * Construct a Builder for a FullBucketRead with a cover context.
         * <p>
         * Note that in case when query executed on the Riak node other than the one specified by the coverage context,
         * nothing will be returned.
         * <p>
         */
        public Builder(Namespace namespace, byte[] coverageContext)
        {
            builder2i = new BuilderFullBucketRead2i(namespace, coverageContext);
        }

        private BuilderFullBucketRead2i get2iBuilder()
        {
            return builder2i;
        }

        /**
         * Return the object (including any siblings).
         * @param returnBody true to return the object.
         * @return a reference to this object.
         */
        public Builder withReturnBody(boolean returnBody)
        {
            builder2i.withReturnBody(returnBody);
            return this;
        }

        /**
         * Set the continuation for this query.
         * <p>
         * The continuation is returned by a previous paginated query.
         * </p>
         * @param continuation
         * @return a reference to this object.
         */
        public Builder withContinuation(BinaryValue continuation)
        {
            builder2i.withContinuation(continuation);
            return this;
        }

        /**
         * Set the maximum number of results returned by the query.
         * @param maxResults the number of results.
         * @return a reference to this object.
         */
        public Builder withMaxResults(Integer maxResults)
        {
            builder2i.withMaxResults(maxResults);
            return this;
        }

        /**
         * Set whether to sort the results of a non-paginated 2i query.
         * <p>
         * Setting this to true will sort the results in Riak before returning them.
         * </p>
         * <p>
         * Note that this is not recommended for queries that could return a large
         * result set; the overhead in Riak is substantial.
         * </p>
         *
         * @param orderByKey true to sort the results, false to return as-is.
         * @return a reference to this object.
         */
        public Builder withPaginationSort(boolean orderByKey)
        {
            builder2i.withPaginationSort(orderByKey);
            return this;
        }

        /**
         * Construct the query.
         * @return a new FullBucketRead
         */
        public FullBucketRead build()
        {
            return new FullBucketRead(this);
        }
    }

    public static class Response extends SecondaryIndexQuery.Response<BinaryValue, Response.Entry>
    {
        private transient List<Entry> convertedList = null;

        protected Response(Namespace queryLocation, IndexConverter<BinaryValue> converter, final int timeout, StreamingRiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(queryLocation, converter, timeout, coreFuture);
        }

        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<BinaryValue> converter)
        {
            super(queryLocation, coreResponse, converter);
        }

        @Override
        protected Entry createEntry(Location location, SecondaryIndexQueryOperation.Response.Entry coreEntry, IndexConverter<BinaryValue> converter)
        {
            final FetchValue.Response fr;
            if (coreEntry.hasBody())
            {
                FetchOperation.Response resp = coreEntry.getBody();

                // The following code has been copied from the FetchValue.executeAsync - CoreFutureAdapter
                fr = new FetchValue.Response.Builder()
                    .withNotFound(resp.isNotFound())
                    .withUnchanged(resp.isUnchanged())
                    .withValues(resp.getObjectList())
                    .withLocation(location) // for ORM
                    .build();
            }
            else
            {
                fr = null;
            }

            return new Entry(location, fr);
        }

        public static class Entry extends SecondaryIndexQuery.Response.Entry<BinaryValue>
        {
            private final FetchValue.Response fetchedValue;

            public Entry(Location location)
            {
                this(location, null);
            }

            public Entry(Location location, FetchValue.Response fetchedResponse)
            {
                super(location, null, null);
                this.fetchedValue = fetchedResponse;
            }

            public boolean hasFetchedValue()
            {
                return fetchedValue != null;
            }

            public FetchValue.Response getFetchedValue()
            {
                return fetchedValue;
            }

            @Override
            public String toString()
            {
                return "FullBucketRead.Response.Entry{" +
                        "location=" + getRiakObjectLocation() +
                        ", hasFetchedValue=" + hasFetchedValue() +
                        '}';
            }
        }
    }
}
