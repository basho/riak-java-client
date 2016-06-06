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

import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Command used to retrieve all values from  Riak bucket.
 * <p>
 * Command might be executed with or without {@link com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry}.
 * If coverage entry/context is provided, the only primary writes that related to the coverage context will be returned,
 * otherwise all data will be returned. All execution options may be used in conjunction
 * with coverage context.
 *
 * Unlike 2i queries, FullBucketRead may return values/objects as part of the response and, as a result, it has better performance.
 * To activate this option you need to set {@link com.basho.riak.client.api.commands.kv.FullBucketRead.Builder#returnBody}
 * parameter.
 *
 * Note that this command mustn't be used without coverage context for querying buckets that contain a big amount of data.
 *
 * @author Sergey Galkin <sgalkin at basho dot com>
 * @see CoveragePlan
 */
public class FullBucketRead extends SecondaryIndexQuery<BinaryValue, FullBucketRead.Response, FullBucketRead>
{
    private final IndexConverter<BinaryValue> converter;

    protected FullBucketRead(Builder builder)
    {
        super(builder.get2iBuilder());
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
    protected RiakFuture<Response, FullBucketRead> executeAsync(RiakCluster cluster)
    {
        RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture =
                executeCoreAsync(cluster);

        RawQueryFuture future = new RawQueryFuture(coreFuture);
        coreFuture.addListener(future);
        return future;
    }

    protected final class RawQueryFuture extends CoreFutureAdapter<Response, FullBucketRead, SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
    {
        public RawQueryFuture(RiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query> coreFuture)
        {
            super(coreFuture);
        }

        @Override
        protected Response convertResponse(SecondaryIndexQueryOperation.Response coreResponse)
        {
            return new Response(namespace, coreResponse, converter);
        }

        @Override
        protected FullBucketRead convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
        {
            return FullBucketRead.this;
        }
    }

    /**
     * Builder used to construct a FullBucketRead command.
     */
    public static class Builder
    {
        private static class BuilderFullBucketRead2i extends  SecondaryIndexQuery.Init<BinaryValue, BuilderFullBucketRead2i>
        {
            public BuilderFullBucketRead2i(Namespace namespace)
            {
                super(namespace, "$bucket", BinaryValue.create(ByteString.EMPTY.toByteArray()));
            }

            public BuilderFullBucketRead2i(Namespace namespace, byte[] coverageContext)
            {
                super(namespace, "$bucket", coverageContext);
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


    public static class Response extends SecondaryIndexQuery.Response<BinaryValue>
    {
        private transient List<Entry> convertedList = null;

        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, SecondaryIndexQuery.IndexConverter<BinaryValue> converter)
        {
            super(queryLocation, coreResponse, converter);
        }

        @Override
        public List<Entry> getEntries()
        {
            if(convertedList != null)
            {
                return convertedList;
            }

            convertedList = new ArrayList<Entry>(coreResponse.getEntryList().size());
            for (SecondaryIndexQueryOperation.Response.Entry e : coreResponse.getEntryList())
            {
                final Location loc = getLocationFromCoreEntry(e);

                final FetchValue.Response fr;
                if (e.hasBody())
                {
                    FetchOperation.Response resp = e.getBody();

                    // The following code has been copied from the FetchValue.executeAsync - CoreFutureAdapter
                    fr = new FetchValue.Response.Builder()
                        .withNotFound(resp.isNotFound())
                        .withUnchanged(resp.isUnchanged())
                        .withValues(resp.getObjectList())
                        .withLocation(loc) // for ORM
                        .build();

                }
                else
                {
                    fr = null;
                }

                Entry ce = new Entry(loc, fr);
                convertedList.add(ce);
            }
            return convertedList;
        }

        public static class Entry
        {
            private final FetchValue.Response fetchedValue;
            private final Location location;

            public Entry(Location location)
            {
                this(location, null);
            }

            public Entry(Location location, FetchValue.Response fetchedResponse)
            {
                this.fetchedValue = fetchedResponse;
                this.location = location;
            }

            public boolean hasFetchedValue()
            {
                return fetchedValue != null;
            }

            public FetchValue.Response getFetchedValue()
            {
                return fetchedValue;
            }

            public Location getLocation()
            {
                return location;
            }

            @Override
            public String toString()
            {
                return "FullBucketRead.Response.Entry{" +
                        "location=" + location +
                        ", hasFetchedValue=" + hasFetchedValue() +
                        '}';
            }
        }
    }
}
