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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public class SecondaryIndexQueryOperation extends FutureOperation<SecondaryIndexQueryOperation.Response, RiakKvPB.RpbIndexResp>
{
    private final RiakKvPB.RpbIndexReq pbReq;
    
    private SecondaryIndexQueryOperation(Builder builder)
    {
        // Yo dawg, we don't ever not want to use streaming.
        builder.pbReqBuilder.setStream(true);
        this.pbReq = builder.pbReqBuilder.build();
    }

    @Override
    protected SecondaryIndexQueryOperation.Response convert(List<RiakKvPB.RpbIndexResp> rawResponse) throws ExecutionException
    {
        SecondaryIndexQueryOperation.Response.Builder responseBuilder = 
            new SecondaryIndexQueryOperation.Response.Builder();
        
        for (RiakKvPB.RpbIndexResp pbEntry : rawResponse)
        {
            /**
             * The 2i API is inconsistent on the Riak side. If it's not 
             * a range query, return_terms is ignored it only returns the 
             * list of object keys and you have to have
             * preserved the index key if you want to return it to the user
             * with the results. 
             */
            
            if (pbReq.getReturnTerms())
            {
                if (pbReq.hasRangeMin())
                {
                    for (RpbPair pair : pbEntry.getResultsList())
                    {
                        responseBuilder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(pair.getKey().toByteArray()), 
                                                             BinaryValue.unsafeCreate(pair.getValue().toByteArray())));
                    }
                }
                else
                {
                    for (ByteString objKey : pbEntry.getKeysList())
                    {
                        responseBuilder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(pbReq.getKey().toByteArray()),
                                                             BinaryValue.unsafeCreate(objKey.toByteArray())));
                    }
                }
            }
            else
            {
                /**
                 * If return_terms wasn't specified only the object keys are returned
                 */
                for (ByteString objKey : pbEntry.getKeysList())
                {
                    responseBuilder.addEntry(new Response.Entry(BinaryValue.unsafeCreate(objKey.toByteArray())));
                }
            }
            
            if (pbEntry.hasContinuation())
            {
                responseBuilder.withContinuation(BinaryValue.unsafeCreate(pbEntry.getContinuation().toByteArray()));
            }
        }
        return responseBuilder.build();
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
            Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_IndexResp);
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
    
    
    /**
     * Builder that constructs a SecondaryIndexQueryOperation.
     */
    public static class Builder
    {
        private final RiakKvPB.RpbIndexReq.Builder pbReqBuilder = RiakKvPB.RpbIndexReq.newBuilder();
        private final Location location;
        
        /**
         * Constructs a builder for a SecondaryIndexQueryOperation. 
         * The index name must be the complete name with the _int or _bin suffix.
         * @param location the location of the index in Riak.
         * @param indexName the name of the index (including suffix).
         */
        public Builder(Location location, 
                        BinaryValue indexName)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            else if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }


            pbReqBuilder.setBucket(ByteString.copyFrom(location.getBucketName().unsafeGetValue()))
                        .setType(ByteString.copyFrom(location.getBucketType().unsafeGetValue()))
                        .setIndex(ByteString.copyFrom(indexName.unsafeGetValue()));
            this.location = location;
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
            pbReqBuilder.setKey(ByteString.copyFrom(key.unsafeGetValue()));
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
            pbReqBuilder.setRangeMin(ByteString.copyFrom(startingIndex.unsafeGetValue()));
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
            pbReqBuilder.setRangeMax(ByteString.copyFrom(endIndex.unsafeGetValue()));
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
            pbReqBuilder.setReturnTerms(returnBoth);
            return this;
        }
        
        /**
         * Set the maximum number of results returned by the query.
         * @param maxResults the number of results.
         * @return a reference to this object.
         */
        public Builder withMaxResults(int maxResults)
        {
            pbReqBuilder.setMaxResults(maxResults);
            return this;
        }
        
        /**
         * Set the continuation for this query.
         * @param continuation the continuation.
         * @return a reference to this object.
         */
        public Builder withContinuation(BinaryValue continuation)
        {
            pbReqBuilder.setContinuation(ByteString.copyFrom(continuation.unsafeGetValue()));
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
            pbReqBuilder.setPaginationSort(orderByKey);
            return this;
        }

        /**
         * Set the regex to filter result terms by for this query.
         * @param filter the regex to filter terms by.
         * @return a reference to this object.
         */
        public Builder withRegexTermFilter(BinaryValue filter)
        {
            pbReqBuilder.setTermRegex(ByteString.copyFrom(filter.unsafeGetValue()));
            return this;
        }
        
        /**
         * Construct a new SecondaryIndexQueryOperation.
         * @return a SecondaryIndexQueryOperation
         */
        public SecondaryIndexQueryOperation build()
        {
            // sanity checks
            if ( !pbReqBuilder.hasRangeMin() && !pbReqBuilder.hasRangeMax() && !pbReqBuilder.hasKey())
            {
                throw new IllegalArgumentException("An index key or range must be supplied");
            }
            else if ( (pbReqBuilder.hasRangeMin() && !pbReqBuilder.hasRangeMax()) ||
                 (pbReqBuilder.hasRangeMax() && !pbReqBuilder.hasRangeMin()) )
            {
                throw new IllegalArgumentException("When specifying ranges both start and end must be asSet");
            }
            else if (pbReqBuilder.hasRangeMin() && pbReqBuilder.hasKey())
            {
                throw new IllegalArgumentException("Cannot specify single index key and range");
            }
            else if (pbReqBuilder.hasMaxResults() && pbReqBuilder.hasPaginationSort()
                                                  && !pbReqBuilder.getPaginationSort())
            {
                throw new IllegalArgumentException("Cannot set paginationSort=false while setting maxResults");
            }
            else if (pbReqBuilder.hasTermRegex() && pbReqBuilder.getIndex().toStringUtf8().endsWith("_int"))
            {
                throw new IllegalArgumentException("Cannot use term regular expression in integer query");
            }

            if (pbReqBuilder.hasKey())
            {
                pbReqBuilder.setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.eq);
            }
            else
            {
                pbReqBuilder.setQtype(RiakKvPB.RpbIndexReq.IndexQueryType.range);
            }
            
            return new SecondaryIndexQueryOperation(this);
            
        }
    }
    
    public static class Response 
    {
        private final BinaryValue continuation;
        private final List<Response.Entry> entryList;

        Response(Builder builder)
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
            private List<Response.Entry> entryList = 
                new ArrayList<Response.Entry>();
            
            Builder()
            {}
            
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
