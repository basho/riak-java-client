/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.StreamingFutureOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public class SecondaryIndexQueryOperation
        extends StreamingFutureOperation<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation
        .Response, RiakKvPB.RpbIndexResp, SecondaryIndexQueryOperation.Query>
{
    private final RiakKvPB.RpbIndexReq pbReq;
    private final Query query;
    private final BlockingQueue<Response> responseQueue;

    private SecondaryIndexQueryOperation(Builder builder)
    {
        // Decide if we should release results as they come in (stream), or gather them all until the operation is
        // done (not stream).
        super(builder.streamResults);

        // Yo dawg, we don't ever not want to use streaming on the
        builder.pbReqBuilder.setStream(true);
        this.query = builder.query;
        this.pbReq = builder.pbReqBuilder.build();
        this.responseQueue = new LinkedBlockingQueue<>();
    }

    @Override
    protected SecondaryIndexQueryOperation.Response convert(List<RiakKvPB.RpbIndexResp> rawResponse)
    {
        SecondaryIndexQueryOperation.Response.Builder responseBuilder =
                new SecondaryIndexQueryOperation.Response.Builder();

        for (RiakKvPB.RpbIndexResp indexResp : rawResponse)
        {
            responseBuilder.addAllEntries(convertEntries(indexResp));

            if (indexResp.hasContinuation())
            {
                responseBuilder.withContinuation(BinaryValue.unsafeCreate(indexResp.getContinuation().toByteArray()));
            }
        }

        return responseBuilder.build();
    }

    private Collection<? extends Response.Entry> convertEntries(RiakKvPB.RpbIndexResp pbEntry)
    {
        /**
         * The 2i API is inconsistent on the Riak side. If it's not
         * a range query, return_terms is ignored it only returns the
         * list of object keys and you have to have
         * preserved the index key if you want to return it to the user
         * with the results.
         *
         * Also, the $key index queries just ignore return_terms altogether.
         */

        List<Response.Entry> entries;

        if (pbReq.getReturnTerms() && !query.indexName.toString().equalsIgnoreCase("$key"))
        {
            if (pbReq.hasRangeMin())
            {
                entries = new ArrayList<>(pbEntry.getResultsCount());
                for (RpbPair pair : pbEntry.getResultsList())
                {
                    entries.add(new Response.Entry(BinaryValue.unsafeCreate(pair.getKey().toByteArray()),
                                                                BinaryValue.unsafeCreate(pair.getValue().toByteArray())));
                }
            }
            else
            {
                entries = new ArrayList<>(pbEntry.getKeysCount());
                for (ByteString objKey : pbEntry.getKeysList())
                {
                    entries.add(new Response.Entry(BinaryValue.unsafeCreate(pbReq.getKey().toByteArray()),
                                                                BinaryValue.unsafeCreate(objKey.toByteArray())));
                }
            }
        }
        else
        {
            /**
             * If return_terms wasn't specified only the object keys are returned
             */
            entries = new ArrayList<>(pbEntry.getKeysCount());

            for (ByteString objKey : pbEntry.getKeysList())
            {
                entries.add(new Response.Entry(BinaryValue.unsafeCreate(objKey.toByteArray())));
            }
        }

        return entries;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_IndexReq, pbReq.toByteArray());
    }

    @Override
    protected RiakKvPB.RpbIndexResp decode(RiakMessage rawMessage)
    {
        try
        {
            Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_IndexResp);
            return RiakKvPB.RpbIndexResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    protected boolean done(RiakKvPB.RpbIndexResp msg)
    {
        return msg.getDone();
    }

    @Override
    public Query getQueryInfo()
    {
        return query;
    }

    @Override
    protected void processStreamingChunk(RiakKvPB.RpbIndexResp rawResponseChunk)
    {
        SecondaryIndexQueryOperation.Response.Builder responseBuilder =
                new SecondaryIndexQueryOperation.Response.Builder();

        responseBuilder.addAllEntries(convertEntries(rawResponseChunk));

        if (rawResponseChunk.hasContinuation())
        {
            // Return the continuation in the normal fashion as well
            final RiakKvPB.RpbIndexResp continuationOnlyResponse = RiakKvPB.RpbIndexResp.newBuilder().setContinuation(
                    rawResponseChunk.getContinuation()).build();
            rawResponses.add(continuationOnlyResponse);

            responseBuilder.withContinuation(BinaryValue.unsafeCreate(rawResponseChunk.getContinuation().toByteArray()));
        }

        final Response response = responseBuilder.build();

        this.responseQueue.add(response);
    }

    @Override
    public BlockingQueue<Response> getResultsQueue()
    {
        return this.responseQueue;
    }


    /**
     * Builder that constructs a QueryOperation.
     */
    public static class Builder
    {
        private final RiakKvPB.RpbIndexReq.Builder pbReqBuilder = RiakKvPB.RpbIndexReq.newBuilder();
        private final Query query;
        private boolean streamResults = false;

        /**
         * Constructs a builder for a QueryOperation.
         * The index name must be the complete name with the _int or _bin suffix.
         *
         * @param query A 2i query.
         */
        public Builder(Query query)
        {
            if (query == null)
            {
                throw new IllegalArgumentException("Query cannot be null.");
            }

            this.query = query;

            pbReqBuilder.setBucket(ByteString.copyFrom(query.namespace.getBucketName().unsafeGetValue())).setType(
                    ByteString.copyFrom(query.namespace.getBucketType().unsafeGetValue())).setIndex(ByteString.copyFrom(
                    query.indexName.unsafeGetValue())).setReturnTerms(query.returnKeyAndIndex);

            if (query.indexKey != null)
            {
                pbReqBuilder.setKey(ByteString.copyFrom(query.indexKey.unsafeGetValue()))
                            .setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.eq);
            }
            else
            {
                pbReqBuilder.setRangeMin(ByteString.copyFrom(query.rangeStart.unsafeGetValue()))
                            .setRangeMax(ByteString.copyFrom(query.rangeEnd.unsafeGetValue()))
                            .setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.range);
            }

            if (query.maxResults != null)
            {
                pbReqBuilder.setMaxResults(query.maxResults);
            }

            if (query.continuation != null)
            {
                pbReqBuilder.setContinuation(ByteString.copyFrom(query.continuation.unsafeGetValue()));
            }

            if (query.paginationSort != null)
            {
                pbReqBuilder.setPaginationSort(query.paginationSort);
            }

            if (query.termFilter != null)
            {
                pbReqBuilder.setTermRegex(ByteString.copyFrom(query.termFilter.unsafeGetValue()));
            }

            if (query.timeout != null)
            {
                pbReqBuilder.setTimeout(query.timeout);
            }
        }

        /**
         * Set the streamResults flag.
         * <p>
         * If unset or false, the entire result set will be available through the {@link ListKeysOperation#get()}
         * method once the operation is complete.
         * <p>
         * If set to true, results will be pushed to the queue available through the
         * {@link ListKeysOperation#getResultsQueue()}
         * method as soon as they are available.
         *
         * @param streamResults whether to stream results to {@link ListKeysOperation#get()}(false), or
         *                      {@link ListKeysOperation#getResultsQueue()}(true)
         * @return A reference to this object.
         */
        public Builder streamResults(boolean streamResults)
        {
            this.streamResults = streamResults;
            return this;
        }

        /**
         * Construct a new QueryOperation.
         *
         * @return a QueryOperation
         */
        public SecondaryIndexQueryOperation build()
        {
            return new SecondaryIndexQueryOperation(this);
        }
    }

    public static class Query
    {
        private final Namespace namespace;
        private final BinaryValue indexName;
        private final BinaryValue indexKey;
        private final BinaryValue rangeStart;
        private final BinaryValue rangeEnd;
        private final boolean returnKeyAndIndex;
        private final Integer maxResults;
        private final BinaryValue continuation;
        private final Boolean paginationSort;
        private final BinaryValue termFilter;
        private final Integer timeout;

        private Query(Builder builder)
        {
            this.indexName = builder.indexName;
            this.indexKey = builder.indexKey;
            this.rangeStart = builder.rangeStart;
            this.rangeEnd = builder.rangeEnd;
            this.returnKeyAndIndex = builder.returnKeyAndIndex;
            this.maxResults = builder.maxResults;
            this.continuation = builder.continuation;
            this.paginationSort = builder.paginationSort;
            this.termFilter = builder.termFilter;
            this.namespace = builder.namespace;
            this.timeout = builder.timeout;
        }

        /**
         * Return the location for the Query.
         *
         * @return the location.
         */
        public Namespace getNamespace()
        {
            return namespace;
        }

        /**
         * @return the indexName
         */
        public BinaryValue getIndexName()
        {
            return indexName;
        }

        /**
         * @return the indexKey
         */
        public BinaryValue getIndexKey()
        {
            return indexKey;
        }

        /**
         * @return the rangeStart
         */
        public BinaryValue getRangeStart()
        {
            return rangeStart;
        }

        /**
         * @return the rangeEnd
         */
        public BinaryValue getRangeEnd()
        {
            return rangeEnd;
        }

        /**
         * @return the returnKeyAndIndex
         */
        public boolean isReturnKeyAndIndex()
        {
            return returnKeyAndIndex;
        }

        /**
         * @return the maxResults
         */
        public int getMaxResults()
        {
            return maxResults;
        }

        /**
         * @return the continuation
         */
        public BinaryValue getContinuation()
        {
            return continuation;
        }

        /**
         * @return the paginationSort
         */
        public boolean isPaginationSort()
        {
            return paginationSort;
        }

        /**
         * @return the termFilter
         */
        public BinaryValue getTermFilter()
        {
            return termFilter;
        }

        /**
         * @return the timeout value, or null if not set.
         */
        public Integer getTimeout()
        {
            return timeout;
        }


        public static class Builder
        {
            private final Namespace namespace;
            private final BinaryValue indexName;
            private BinaryValue indexKey;
            private BinaryValue rangeStart;
            private BinaryValue rangeEnd;
            private boolean returnKeyAndIndex;
            private Integer maxResults;
            private BinaryValue continuation;
            private Boolean paginationSort;
            private BinaryValue termFilter;
            private Integer timeout;

            /**
             * Constructs a builder for a (2i) Query.
             * The index name must be the complete name with the _int or _bin suffix.
             *
             * @param namespace the namespace for this Query
             * @param indexName the name of the index (including suffix).
             */
            public Builder(Namespace namespace, BinaryValue indexName)
            {
                if (namespace == null)
                {
                    throw new IllegalArgumentException("Namespace cannot be null");
                }
                else if (null == indexName || indexName.length() == 0)
                {
                    throw new IllegalArgumentException("Index name cannot be null or zero length");
                }
                this.indexName = indexName;
                this.namespace = namespace;
            }

            /**
             * Set a single secondary index key to use for query.
             * If querying a _int index the bytes must be the UTF-8 text
             * representation of an integer (Yes, really).
             *
             * @param key the secondary index key.
             * @return a reference to this object.
             */
            public Builder withIndexKey(BinaryValue key)
            {
                this.indexKey = key;
                return this;
            }

            /**
             * Set the start value for a range query.
             * If querying a _int index the bytes must be the UTF-8 text
             * representation of an integer (Yes, really).
             *
             * @param startingIndex the starting index for a range query.
             * @return a reference to this object.
             */
            public Builder withRangeStart(BinaryValue startingIndex)
            {
                this.rangeStart = startingIndex;
                return this;
            }

            /**
             * Set the ending value for a range query.
             * If querying a _int index the bytes must be the UTF-8 text
             * representation of an integer (Yes, really).
             *
             * @param endIndex the ending index for a range query.
             * @return a reference to this object.
             */
            public Builder withRangeEnd(BinaryValue endIndex)
            {
                this.rangeEnd = endIndex;
                return this;
            }

            /**
             * Set whether to return the index keys with the Riak object keys.
             * Setting this to true will return both the index key and the Riak
             * object's key. The default is false (only to return the Riak object keys).
             *
             * @param returnBoth true to return both index and object keys, false to return only object keys.
             * @return a reference to this object.
             */
            public Builder withReturnKeyAndIndex(boolean returnBoth)
            {
                this.returnKeyAndIndex = returnBoth;
                return this;
            }

            /**
             * Set the maximum number of results returned by the query.
             *
             * @param maxResults the number of results.
             * @return a reference to this object.
             */
            public Builder withMaxResults(int maxResults)
            {
                this.maxResults = maxResults;
                return this;
            }

            /**
             * Set the continuation for this query.
             *
             * @param continuation the continuation.
             * @return a reference to this object.
             */
            public Builder withContinuation(BinaryValue continuation)
            {
                this.continuation = continuation;
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
                this.paginationSort = orderByKey;
                return this;
            }

            /**
             * Set the regex to filter result terms by for this query.
             *
             * @param filter the regex to filter terms by.
             * @return a reference to this object.
             */
            public Builder withRegexTermFilter(BinaryValue filter)
            {
                this.termFilter = filter;
                return this;
            }

            /**
             * Set the timeout for the query.
             * <p>
             * Sets the server-side timeout value for this query.
             * </p>
             *
             * @param timeout
             * @return a reference to this object.
             */
            public Builder withTimeout(int timeout)
            {
                this.timeout = timeout;
                return this;
            }

            public Query build()
            {
                // sanity checks
                if (rangeStart == null && rangeEnd == null && indexKey == null)
                {
                    throw new IllegalArgumentException("An index key or range must be supplied");
                }
                else if ((rangeStart != null && rangeEnd == null) || (rangeEnd != null && rangeStart == null))
                {
                    throw new IllegalArgumentException("When specifying ranges both start and end must be Set");
                }
                else if (rangeStart != null && indexKey != null)
                {
                    throw new IllegalArgumentException("Cannot specify single index key and range");
                }
                else if (maxResults != null && (paginationSort != null && !paginationSort))
                {
                    throw new IllegalArgumentException("Cannot set paginationSort=false while setting maxResults");
                }
                else if (termFilter != null && indexName.toStringUtf8().endsWith("_int"))
                {
                    throw new IllegalArgumentException("Cannot use term regular expression in integer query");
                }

                return new Query(this);
            }
        }
    }

    public static class Response
    {
        private final BinaryValue continuation;
        private final List<Response.Entry> entryList;

        private Response(Builder builder)
        {
            this.continuation = builder.continuation;
            this.entryList = builder.entryList;
        }

        public boolean hasContinuation()
        {
            return continuation != null;
        }

        public BinaryValue getContinuation()
        {
            return continuation;
        }

        public List<Response.Entry> getEntryList()
        {
            return entryList;
        }

        public static class Entry
        {
            private final BinaryValue indexKey;
            private final BinaryValue objectKey;

            Entry(BinaryValue objectKey)
            {
                this(null, objectKey);
            }

            Entry(BinaryValue indexKey, BinaryValue objectKey)
            {
                this.indexKey = indexKey;
                this.objectKey = objectKey;
            }

            public boolean hasIndexKey()
            {
                return indexKey != null;
            }

            public BinaryValue getIndexKey()
            {
                return indexKey;
            }

            public BinaryValue getObjectKey()
            {
                return objectKey;
            }
        }

        static class Builder
        {
            private BinaryValue continuation;
            private List<Response.Entry> entryList = new ArrayList<Response.Entry>();

            Builder withContinuation(BinaryValue continuation)
            {
                this.continuation = continuation;
                return this;
            }

            Builder addEntry(Response.Entry entry)
            {
                entryList.add(entry);
                return this;
            }

            Builder addAllEntries(Collection<? extends Entry> entries)
            {
                entryList.addAll(entries);
                return this;
            }

            Response build()
            {
                return new Response(this);
            }
        }

    }

}
