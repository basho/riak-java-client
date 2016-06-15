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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public class SecondaryIndexQueryOperation extends FutureOperation<SecondaryIndexQueryOperation.Response, Object, SecondaryIndexQueryOperation.Query>
{
    private final static Logger logger = LoggerFactory.getLogger(SecondaryIndexQueryOperation.class);
    private final RiakKvPB.RpbIndexReq pbReq;
    private final Query query;

    private SecondaryIndexQueryOperation(Builder builder)
    {
        // Yo dawg, we don't ever not want to use streaming.
        builder.pbReqBuilder.setStream(true);
        this.query = builder.query;
        this.pbReq = builder.pbReqBuilder.build();
    }

    @Override
    protected SecondaryIndexQueryOperation.Response convert(List<Object> rawResponse)
    {
        SecondaryIndexQueryOperation.Response.Builder responseBuilder =
                new SecondaryIndexQueryOperation.Response.Builder();

        for (Object o : rawResponse)
        {
            if (o instanceof RiakKvPB.RpbIndexBodyResp)
            {
                assert pbReq.getReturnBody();
                final RiakKvPB.RpbIndexBodyResp bodyResp = (RiakKvPB.RpbIndexBodyResp)o;
                convertBodies(responseBuilder, bodyResp);

                if (bodyResp.hasContinuation())
                {
                    responseBuilder.withContinuation(BinaryValue.unsafeCreate(bodyResp.getContinuation().toByteArray()));
                }
                continue;
            }

            final RiakKvPB.RpbIndexResp pbEntry = (RiakKvPB.RpbIndexResp) o;

            /**
             * The 2i API is inconsistent on the Riak side. If it's not
             * a range query, return_terms is ignored it only returns the
             * list of object keys and you have to have
             * preserved the index key if you want to return it to the user
             * with the results.
             *
             * Also, the $key index queries just ignore return_terms altogether.
             */

            if (pbReq.getReturnTerms() && !query.indexName.toString().equalsIgnoreCase("$key"))
            {
                convertTerms(responseBuilder, pbEntry);
            }
            else
            {
                convertKeys(responseBuilder, pbEntry);
            }

            if (pbEntry.hasContinuation())
            {
                responseBuilder.withContinuation(BinaryValue.unsafeCreate(pbEntry.getContinuation().toByteArray()));
            }
        }
        return responseBuilder.build();
    }

    private static void convertKeys(SecondaryIndexQueryOperation.Response.Builder builder, RiakKvPB.RpbIndexResp pbEntry)
    {
        /**
         * If return_terms wasn't specified only the object keys are returned
         */
        for (ByteString objKey : pbEntry.getKeysList())
        {
            builder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(objKey.toByteArray())));
        }
    }

    private static void convertBodies(SecondaryIndexQueryOperation.Response.Builder builder, RiakKvPB.RpbIndexBodyResp resp)
    {
        for (RiakKvPB.RpbIndexObject io: resp.getObjectsList())
        {
            final FetchOperation.Response fr = FetchOperation.convert(io.getObject());
            builder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(io.getKey().toByteArray()), fr));
        }
    }

    private void convertTerms(SecondaryIndexQueryOperation.Response.Builder builder, RiakKvPB.RpbIndexResp pbEntry)
    {
        if (pbReq.hasRangeMin())
        {
            for (RpbPair pair : pbEntry.getResultsList())
            {
                builder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(pair.getKey().toByteArray()),
                        BinaryValue.unsafeCreate(pair.getValue().toByteArray())));
            }
        }
        else
        {
            for (ByteString objKey : pbEntry.getKeysList())
            {
                builder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(pbReq.getKey().toByteArray()),
                        BinaryValue.unsafeCreate(objKey.toByteArray())));
            }
        }
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_IndexReq, pbReq.toByteArray());
    }

    @Override
    protected Object decode(RiakMessage rawMessage)
    {
        try
        {
            if (rawMessage.getCode() == RiakMessageCodes.MSG_IndexResp)
            {
                return RiakKvPB.RpbIndexResp.parseFrom(rawMessage.getData());
            }
            else if (rawMessage.getCode() == RiakMessageCodes.MSG_IndexBodyResp)
            {
                return RiakKvPB.RpbIndexBodyResp.parseFrom(rawMessage.getData());
            }
            throw new IllegalArgumentException("Invalid message received: Wrong response; expected "
                    + RiakMessageCodes.MSG_IndexResp + " or " + RiakMessageCodes.MSG_IndexBodyResp
                    + " received " + rawMessage.getCode());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    protected boolean done(Object msg)
    {
        if (msg instanceof RiakKvPB.RpbIndexResp)
        {
            return ((RiakKvPB.RpbIndexResp)msg).getDone();
        }
        else if (msg instanceof RiakKvPB.RpbIndexBodyResp)
        {
            return ((RiakKvPB.RpbIndexBodyResp)msg).getDone();
        }

        throw new IllegalStateException("Unsupported response message type");
    }

    @Override
    public Query getQueryInfo()
    {
        return query;
    }


    /**
     * Builder that constructs a QueryOperation.
     */
    public static class Builder
    {
        private final RiakKvPB.RpbIndexReq.Builder pbReqBuilder = RiakKvPB.RpbIndexReq.newBuilder();
        private final Query query;

        /**
         * Constructs a builder for a QueryOperation.
         * The index name must be the complete name with the _int or _bin suffix.
         * @param query A 2i query.
         */
        public Builder(Query query)
        {
            if (query == null)
            {
                throw new IllegalArgumentException("Query cannot be null.");
            }

            /**
             * Options 'returnBody' and 'returnKeyAndIndex' are contradictory because them both use the same field
             * to store the results
             */
            if (query.returnBody && query.returnKeyAndIndex)
            {
                throw new IllegalArgumentException("Contradictory query options: returnBody and returnKeyAndIndex");
            }

            this.query = query;

            pbReqBuilder.setBucket(ByteString.copyFrom(query.namespace.getBucketName().unsafeGetValue()))
                        .setType(ByteString.copyFrom(query.namespace.getBucketType().unsafeGetValue()))
                        .setIndex(ByteString.copyFrom(query.indexName.unsafeGetValue()))
                        .setReturnTerms(query.returnKeyAndIndex)
                        .setReturnBody(query.returnBody);
            
            if (query.indexKey != null)
            {
                pbReqBuilder.setKey(ByteString.copyFrom(query.indexKey.unsafeGetValue()))
                            .setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.eq);
            }
            else if(query.getRangeStart() != null)
            {
                pbReqBuilder.setRangeMin(ByteString.copyFrom(query.rangeStart.unsafeGetValue()))
                            .setRangeMax(ByteString.copyFrom(query.rangeEnd.unsafeGetValue()))
                            .setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.range);
            }
            else
            {
                // Full Bucket Read
                assert query.coverageContext != null;

                pbReqBuilder.setCoverContext(ByteString.copyFrom(query.coverageContext))
                    .setKey(ByteString.EMPTY)
                    .setIndex(ByteString.copyFromUtf8("$bucket"))
                    .clearReturnTerms()
                    .setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.eq);
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

            if (query.coverageContext != null)
            {
                pbReqBuilder.setCoverContext(ByteString.copyFrom(query.coverageContext));
            }
        }

        /**
         * Construct a new QueryOperation.
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
        private final byte[] coverageContext;
        private final boolean returnBody;

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
            this.coverageContext = builder.coverageContext;
            this.returnBody = builder.returnBody;
        }

        /**
         * Return the location for the Query.
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

        /**
         * @return the cover context value, or null if not set.
         */
        public byte[] getCoverageContext()
        {
            return coverageContext;
        }

        /**
         * @return the returnBody
         */
        public boolean isReturnBody()
        {
            return returnBody;
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
            private byte[] coverageContext;
            private boolean returnBody;
            
            /**
            * Constructs a builder for a (2i) Query.
            * The index name must be the complete name with the _int or _bin suffix.
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
            * @param timeout
            * @return a reference to this object.
            */
           public Builder withTimeout(int timeout)
           {
               this.timeout = timeout;
               return this;
           }

           /**
            * Set the coverage context for the query
            * @param coverageContext
            * @return a reference to this object.
            */
           public Builder withCoverageContext(byte[] coverageContext)
           {
               this.coverageContext = coverageContext;
               return this;
           }

            /**
             * Set whether to return list of RiakObjects inside the response.
             * This option could be used only if target Riak instance supports parallel extract
             * feature (such as RiakTS).
             *
             * @param returnBody true to return data inside response
             * @return a reference to this object.
             */
            public Builder withReturnBody(boolean returnBody)
            {
                this.returnBody = returnBody;
                return this;
            }

           public Query build()
            {
                // sanity checks
                if ( rangeStart == null && rangeEnd == null && indexKey == null && coverageContext == null)
                {
                    throw new IllegalArgumentException("An index key or range or coverageContext must be supplied");
                }
                else if ( (rangeStart != null && rangeEnd == null) ||
                     (rangeEnd != null && rangeStart == null ) )
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
            private final FetchOperation.Response fetchResponse;

            Entry(BinaryValue objectKey, FetchOperation.Response fr)
            {
                this(null, objectKey, fr);
            }

            Entry(BinaryValue objectKey)
            {
                this(null, objectKey);
            }

            Entry(BinaryValue indexKey, BinaryValue objectKey)
            {
                this(indexKey, objectKey, null);
            }

            Entry(BinaryValue indexKey, BinaryValue objectKey, FetchOperation.Response fr)
            {
                this.indexKey = indexKey;
                this.objectKey = objectKey;
                this.fetchResponse = fr;
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

            public boolean hasBody()
            {
                return fetchResponse != null;
            }

            public FetchOperation.Response getBody()
            {
                return fetchResponse;
            }
        }

        static class Builder
        {
            private BinaryValue continuation;
            private List<Response.Entry> entryList =
                new ArrayList<Response.Entry>();

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

            Response build()
            {
                return new Response(this);
            }
        }

    }

}
