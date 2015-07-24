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

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.List;

/**
     * A Secondary Index Query.
     * <p>
     * Serves as a base class for all 2i queries.
     * <p>
     * @param <S> the type being used for the query.
     * 
     * @author Brian Roach <roach at basho dot com>
     * @since 2.0
     */
public abstract class SecondaryIndexQuery<T,S,U> extends RiakCommand<S, U> 
{
    public enum Type
    {
        _INT("_int"), _BIN("_bin"), _BUCKET(""), _KEY("");
        
        private String suffix;
        
        Type(String suffix)
        {
            this.suffix = suffix;
        }
        
        @Override
        public String toString()
        {
            return suffix;
        }
    }
    
    
    protected final Namespace namespace;
    protected final String indexName;
    protected final BinaryValue continuation;
    protected final T match;
    protected final T start;
    protected final T end;
    protected final Integer maxResults;
    protected final boolean returnTerms;
    protected final boolean paginationSort;
    protected final String termFilter;
    protected Integer timeout;
    protected final byte[] coverageContext;

    protected abstract IndexConverter<T> getConverter();

    protected SecondaryIndexQuery(Init<T,?> builder)
    {
        this.namespace = builder.namespace;
        this.indexName = builder.indexName;
        this.continuation = builder.continuation;
        this.match = builder.match;
        this.start = builder.start;
        this.end = builder.end;
        this.maxResults = builder.maxResults;
        this.returnTerms = builder.returnTerms;
        this.paginationSort = builder.paginationSort;
        this.termFilter = builder.termFilter;
        this.timeout = builder.timeout;
        this.coverageContext = builder.coverageContext;
    }

    /**
     * Get the location for this query.
     * @return the location encompassing a bucket and bucket type.
     */
    public Namespace getNamespace()
    {
        return namespace;
    }

    /**
     * Get the full index name for this query.
     * @return the index name including Riak suffix.
     */
    public String getIndexName()
    {
        return indexName;
    }

    /**
     * Get the match value supplied for this query.
     * @return the single index key to match, or null if not present
     */
    public T getMatchValue()
    {
        return match;
    }

    /**
     * Get the range start value for this query.
     * @return the range start, or null if not present.
     */
    public T getRangeStart()
    {
        return start;
    }

    /**
     * Get the range end value for this query.
     * @return the range end value, or null if not present
     */
    public T getRangeEnd()
    {
        return end;
    }

    /**
     * Get the max number of results for this query.
     * @return the max number of results, or null if not present.
     */
    public Integer getMaxResults()
    {
        return maxResults;
    }

    /**
     * Get whether this query will return both index keys and object keys. 
     * @return true if specified, false otherwise.
     */
    public boolean getReturnKeyAndIndex()
    {
        return returnTerms;
    }

    /**
     * Get the pagination sort setting.
     * @return true if set, false otherwise.
     */
    public boolean getPaginationSort()
    {
        return paginationSort;
    }

    /**
     * Get the regex term filter for this query.
     * @return the filter, or null if not set.
     */
    public String getTermFilter()
    {
        return termFilter;
    }

    /**
     * Get the continuation supplied for this query.
     * @return the continuation, or null if not set.
     */
    public BinaryValue getContinuation()
    {
        return continuation;
    }

    /**
     * Get the timeout value for this query.
     * @return the timeout value, or null if not set.
     */
    public Integer getTimeout()
    {
        return timeout;
    }
    
    protected final SecondaryIndexQueryOperation.Query createCoreQuery()
    {
        IndexConverter<T> converter = getConverter();

        SecondaryIndexQueryOperation.Query.Builder coreQueryBuilder =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, BinaryValue.create(indexName))
                .withContinuation(continuation)
                .withReturnKeyAndIndex(returnTerms)
                .withPaginationSort(paginationSort);

        if (termFilter != null)
        {
            coreQueryBuilder.withRegexTermFilter(BinaryValue.create(termFilter));
        }

        if (match != null)
        {
            coreQueryBuilder.withIndexKey(converter.convert(match));
        }
        else
        {
            coreQueryBuilder.withRangeStart(converter.convert(start))
                            .withRangeEnd(converter.convert(end));
        }

        if (maxResults != null)
        {
            coreQueryBuilder.withMaxResults(maxResults);
        }
        
        if (timeout != null)
        {
            coreQueryBuilder.withTimeout(timeout);
        }

        if (coverageContext != null)
        {
            coreQueryBuilder.withCoverageContext(coverageContext);
        }
        return coreQueryBuilder.build();
    }

    protected RiakFuture<SecondaryIndexQueryOperation.Response, 
                        SecondaryIndexQueryOperation.Query> executeCoreAsync(RiakCluster cluster)
    {
        SecondaryIndexQueryOperation.Builder builder =
            new SecondaryIndexQueryOperation.Builder(this.createCoreQuery());
        
        return cluster.execute(builder.build());
    }
                        
    protected interface IndexConverter<T>
    {
        T convert(BinaryValue input);
        BinaryValue convert(T input);
    }
    
    public static abstract class Init<S, T extends Init<S,T>>
    {
        private final Namespace namespace;
        private final String indexName;
        private volatile BinaryValue continuation;
        private volatile S match;
        private volatile S start;
        private volatile S end;
        private volatile Integer maxResults;
        private volatile boolean returnTerms;
        private volatile boolean paginationSort;
        private volatile String termFilter;
        private volatile Integer timeout;
        private volatile byte[] coverageContext;

        protected abstract T self();

        /**
         * Build a range query.
         * <p>
         * Returns all objects in Riak that have an index value
         * in the specified range.
         * </p>
         * @param namespace the namespace for this query.
         * @param indexName the indexname
         * @param start the start index value
         * @param end the end index value
         */
        public Init(Namespace namespace, String indexName, S start, S end)
        {
            this.namespace = namespace;
            this.indexName = indexName;
            this.start = start;
            this.end = end;
        }

        /**
         * Build a match query.
         * <p>
         * Returns all objects in Riak that have an index value matching the
         * one supplied.
         * </p>
         * @param namespace the namespace for this query
         * @param indexName the index name
         * @param match the index value.
         */
        public Init(Namespace namespace, String indexName, S match)
        {
            this.namespace = namespace;
            this.indexName = indexName;
            this.match = match;
        }

        // TODO: Documentation should provide more details about coverageContext
        /**
         * Build a cover query.
         * <p>
         * Returns all objects in Riak related to the provided coverageContext.
         * </p>
         * @param namespace the namespace for this query
         * @param indexName the index name
         * @param coverageContext the cover context.
         */
        public Init(Namespace namespace, String indexName, byte[] coverageContext)
        {
            this.namespace = namespace;
            this.indexName = indexName;
            this.coverageContext = coverageContext;
        }

        /**
         * Set the continuation for this query.
         * <p>
         * The continuation is returned by a previous paginated query.  
         * </p>
         * @param continuation
         * @return a reference to this object.
         */
        public T withContinuation(BinaryValue continuation)
        {
            this.continuation = continuation;
            return self();
        }

        /**
         * Set the maximum number of results returned by the query.
         * @param maxResults the number of results.
         * @return a reference to this object.
         */
        public T withMaxResults(Integer maxResults)
        {
            this.maxResults = maxResults;
            return self();            
        }

        /**
         * Set whether to return the index keys with the Riak object keys.
         * Setting this to true will return both the index key and the Riak
         * object's key. The default is false (only to return the Riak object keys).
         * @param returnBoth true to return both index and object keys, false to return only object keys.
         * @return a reference to this object.
         */
        public T withKeyAndIndex(boolean returnBoth)
        {
            this.returnTerms = returnBoth;
            return self();
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
        public T withPaginationSort(boolean orderByKey)
        {
            this.paginationSort = orderByKey;
            return self();
        }

        /**
         * Set the regex to filter result terms by for this query.
         * @param filter the regex to filter terms by.
         * @return a reference to this object.
         */
        public T withRegexTermFilter(String filter)
        {
            this.termFilter = filter;
            return self();
        }
        
        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for both the 
         * fetch and store operation.
         * </p>
         * @param timeout the timeout in milliseconds
         * @return a reference to this object.
         */
        public T withTimeout(int timeout)
        {
            this.timeout = timeout;
            return self();
        }

        /**
         * Set the cover context for the local read
         * @param coverageContext
         * @return
         */
        public T withCoverageContext(byte[] coverageContext){
            this.coverageContext = coverageContext;
            return self();
        }
    }
    
    /**
     * Base class for all 2i responses.
     * @param <T> The type contained in the resposne.
     */
    public abstract static class Response<T> 
    {
        final IndexConverter<T> converter;
        final SecondaryIndexQueryOperation.Response coreResponse;
        final Namespace queryLocation;
        
        protected Response(Namespace queryLocation, SecondaryIndexQueryOperation.Response coreResponse, IndexConverter<T> converter)
        {
            this.coreResponse = coreResponse;
            this.converter = converter;
            this.queryLocation = queryLocation;
        }
        
        /**
         * Check if this response has a continuation.
         * @return true if the response contains a continuation.
         */
        public boolean hasContinuation()
        {
            return coreResponse.hasContinuation();
        }
        
        /**
         * Get the continuation from this response.
         * @return the continuation, or null if none is present.
         */
        public BinaryValue getContinuation()
        {
            return coreResponse.getContinuation();
        }
        
        /**
         * Check is this response contains any entries.
         * @return true if entries are present, false otherwise.
         */
        public boolean hasEntries()
        {
            return !coreResponse.getEntryList().isEmpty();
        }
        
        
        protected final Location getLocationFromCoreEntry(SecondaryIndexQueryOperation.Response.Entry e)
        {
            Location loc = new Location(queryLocation, e.getObjectKey());
            return loc;
        }
                
        public abstract List<?> getEntries();
        
        public abstract static class Entry<T>
        {
            private final Location RiakObjectLocation;
            private final BinaryValue indexKey;
            private final IndexConverter<T> converter;
            
            protected Entry(Location riakObjectLocation, BinaryValue indexKey, IndexConverter<T> converter)
            {
                this.RiakObjectLocation = riakObjectLocation;
                this.indexKey = indexKey;
                this.converter = converter;
            }
            
            /**
             * Get the location for this entry.
             * @return the location for this object in Riak.
             */
            public Location getRiakObjectLocation()
            {
                return RiakObjectLocation;
            }
            
            /**
             * Get this 2i key for this entry.
             * Note this will only be present if the {@literal withKeyAndIndex(true)} 
             * method was used when constructing the query. 
             * @return The 2i key for this entry or null if not present.
             */
            public T getIndexKey()
            {
                return converter.convert(indexKey);
            }
            
        }
        
    }
}
