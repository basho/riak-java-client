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
package com.basho.riak.client.http.request;

import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.http.util.ClientUtils;

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
    private String continuation;
    
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
    
    public void setContinuation(String continuation)
    {
        this.continuation = continuation;
    }

    public String makeURIString(RiakConfig config)
    {
        StringBuilder sb = new StringBuilder(config.getBaseUrl())
                                .append("/buckets/")
                                .append(ClientUtils.urlEncode(bucketName))
                                .append("/index/")
                                .append(indexName)
                                .append("/");
        
        if (indexKey != null)
        {
            sb.append(indexKey);
        }
        else
        {
            sb.append(rangeStart)
              .append("/")  
              .append(rangeEnd);
        }
        
        return sb.toString();
    }
    
    
    /**
     * @return the returnTerms
     */
    public boolean isReturnTerms()
    {
        return returnTerms;
    }

    public boolean hasMaxResults()
    {
        return maxResults != null;
    }
    
    /**
     * @return the maxResults
     */
    public Integer getMaxResults()
    {
        return maxResults;
    }

    public boolean hasContinuation()
    {
        return continuation != null;
    }
    
    /**
     * @return the continuation
     */
    public String getContinuation()
    {
        return continuation;
    }
    
    public String getIndexKey()
    {
        return indexKey;
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
        private String continuation;
        
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
        
        public Builder withContinuation(String continuation)
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
