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
package com.basho.riak.pbc;

import com.basho.riak.protobuf.RiakKvPB.RpbIndexReq;
import com.google.protobuf.ByteString;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class IndexRequest
{
    private final String bucketName;
    private final String indexName;
    private final String indexKey;
    private final String rangeStart;
    private final String rangeEnd;
    private final boolean returnTerms;
    private final Integer maxResults;
    private ByteString continuation;
    
    
    private IndexRequest(Builder builder)
    {
        this.bucketName = builder.bucketName;
        this.indexName = builder.indexName;
        this.indexKey = builder.indexKey;
        this.rangeStart = builder.rangeStart;
        this.rangeEnd = builder.rangeEnd;
        this.returnTerms = builder.returnTerms;
        this.maxResults = builder.maxResults;
        this.continuation = builder.continuation;
        
    }
    
    public void setContinuation(ByteString continuation)
    {
        this.continuation = continuation;
    }

    public RpbIndexReq buildProtocolBufferReq()
    {
        RpbIndexReq.Builder builder = RpbIndexReq.newBuilder();
        builder.setBucket(ByteString.copyFromUtf8(bucketName))
                .setIndex(ByteString.copyFromUtf8(indexName))
                .setStream(true);
        
        if (indexKey != null) 
        {
            builder.setKey(ByteString.copyFromUtf8(indexKey));
            builder.setQtype(RpbIndexReq.IndexQueryType.eq);
        }
        else
        {
            builder.setRangeMin(ByteString.copyFromUtf8(rangeStart));
            builder.setRangeMax(ByteString.copyFromUtf8(rangeEnd));
            builder.setQtype(RpbIndexReq.IndexQueryType.range);
        }
        
        builder.setReturnTerms(returnTerms);
        if (maxResults != null)
        {
            builder.setMaxResults(maxResults);
        }
        
        if (continuation != null)
        {
            builder.setContinuation(continuation);
        }
        
        return builder.build();
        
        
    }
    
    public boolean returnTerms()
    {
        return returnTerms;
    }
    
    public static class Builder 
    {
        private final String bucketName;
        private final String indexName;
        private String indexKey;
        private String rangeStart;
        private String rangeEnd;
        private boolean returnTerms;
        private Integer maxResults;
        private ByteString continuation;
        
        public Builder (String bucketName, String indexName)
        {
            if ( (bucketName == null || bucketName.equalsIgnoreCase("")) ||
                 (indexName == null || indexName.equalsIgnoreCase("")) )
            {
                throw new IllegalArgumentException("Bucket or Index can not be null or empty");
            }
            
            this.bucketName = bucketName;
            this.indexName = indexName;
        }
        
        public Builder withIndexKey(String key)
        {
            this.indexKey = key;
            return this;
        }
        
        public Builder withIndexKey(long key)
        {
            this.indexKey = String.valueOf(key);
            return this;
        }
        
        public Builder withRangeStart(String startingIndex)
        {
            this.rangeStart = startingIndex;
            return this;
        }
        
        public Builder withRangeStart(long startIndex)
        {
            this.rangeStart = String.valueOf(startIndex);
            return this;
        }
        
        public Builder withRangeEnd(String endIndex) 
        {
            this.rangeEnd = endIndex;
            return this;
        }
        
        public Builder withRangeEnd(long endIndex)
        {
            this.rangeEnd = String.valueOf(endIndex);
            return this;
        }
        
        public Builder withReturnKeyAndIndex(boolean returnBoth)
        {
            this.returnTerms = returnBoth;
            return this;
        }
        
        public Builder withMaxResults(Integer maxResults)
        {
            this.maxResults = maxResults;
            return this;
        }
        
        public Builder withContinuation(ByteString continuation)
        {
            this.continuation = continuation;
            return this;
        }
        
        public IndexRequest build()
        {
            // sanity checks
            if ( rangeStart == null && rangeEnd == null && indexKey == null)
            {
                throw new IllegalArgumentException("An index key or range must be supplied");
            }
            else if ( (rangeStart != null && rangeEnd == null) ||
                 (rangeEnd != null && rangeStart == null) )
            {
                throw new IllegalArgumentException("When specifying ranges both start and end must be set");
            }
            
            return new IndexRequest(this);
            
        }
    }
    
}
