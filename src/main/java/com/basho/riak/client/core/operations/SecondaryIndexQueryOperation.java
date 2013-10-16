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
import com.basho.riak.client.query.indexes.SecondaryIndexEntry;
import com.basho.riak.client.query.indexes.SecondaryIndexQueryResponse;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class SecondaryIndexQueryOperation extends FutureOperation<SecondaryIndexQueryResponse, RiakKvPB.RpbIndexResp>
{
    private final RiakKvPB.RpbIndexReq pbReq;
    
    public SecondaryIndexQueryOperation(Builder builder)
    {
        // Yo dawg, we don't ever not want to use streaming.
        builder.pbReqBuilder.setStream(true);
        this.pbReq = builder.pbReqBuilder.build();
    }

    @Override
    protected SecondaryIndexQueryResponse convert(List<RiakKvPB.RpbIndexResp> rawResponse) throws ExecutionException
    {
        SecondaryIndexQueryResponse response = new SecondaryIndexQueryResponse();
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
                        response.add(new SecondaryIndexEntry(ByteArrayWrapper.unsafeCreate(pair.getKey().toByteArray()), 
                                                             ByteArrayWrapper.unsafeCreate(pair.getValue().toByteArray())));
                    }
                }
                else
                {
                    for (ByteString objKey : pbEntry.getKeysList())
                    {
                        response.add(new SecondaryIndexEntry(ByteArrayWrapper.unsafeCreate(pbReq.getKey().toByteArray()),
                                                             ByteArrayWrapper.unsafeCreate(objKey.toByteArray())));
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
                    response.add(new SecondaryIndexEntry(ByteArrayWrapper.unsafeCreate(objKey.toByteArray())));
                }
            }
            
            if (pbEntry.hasContinuation())
            {
                response.setContinuation(ByteArrayWrapper.unsafeCreate(pbEntry.getContinuation().toByteArray()));
            }
        }
        return response;
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
        
        /**
         * Constructs a builder using the supplied bucket name and index name. 
         * The index name must be the complete name with the _int or _bin suffix.
         * @param bucketName the name of the bucket.
         * @param indexName the name of the index (including suffix).
         */
        public Builder(ByteArrayWrapper bucketName, 
                        ByteArrayWrapper indexName)
        {
            if (null == bucketName || bucketName.length() == 0)
            {
                throw new IllegalArgumentException("Bucket name cannot be null or zero length");
            }
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }


            pbReqBuilder.setBucket(ByteString.copyFrom(bucketName.unsafeGetValue()))
                        .setIndex(ByteString.copyFrom(indexName.unsafeGetValue()));
        }
        
        /**
         * Set the Bucket Type for the query.
         * If not set the default type is used. 
         * @param bucketType the bucket type.
         * @return a reference to this object.
         */
        public Builder withBucketType(ByteArrayWrapper bucketType)
        {
            if (null == bucketType || bucketType.length() == 0)
            {
                throw new IllegalArgumentException("Bucket type cannot be null or zero length");
            }
            pbReqBuilder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
            return this;
        }
        
        /**
         * Set a single secondary index key to use for query.
         * If querying a _int index the bytes must be the UTF-8 text
         * representation of an integer (Yes, really). 
         * @param key the secondary index key.
         * @return a reference to this object.
         */
        public Builder withIndexKey(ByteArrayWrapper key)
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
        public Builder withRangeStart(ByteArrayWrapper startingIndex)
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
        public Builder withRangeEnd(ByteArrayWrapper endIndex) 
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
        public Builder withContinuation(ByteArrayWrapper continuation)
        {
            pbReqBuilder.setContinuation(ByteString.copyFrom(continuation.unsafeGetValue()));
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
                throw new IllegalArgumentException("When specifying ranges both start and end must be set");
            }
            else if (pbReqBuilder.hasRangeMin() && pbReqBuilder.hasKey())
            {
                throw new IllegalArgumentException("Cannot specify single index key and range");
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
    
    
}
