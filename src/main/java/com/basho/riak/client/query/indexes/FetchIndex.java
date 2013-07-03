/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.IndexEntry;
import java.util.List;
import java.util.concurrent.Callable;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.query.StreamingOperation;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.query.IndexSpec;
import com.basho.riak.client.raw.query.indexes.BinRangeQuery;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IntRangeQuery;
import com.basho.riak.client.raw.query.indexes.IntValueQuery;
import java.io.IOException;

/**
 * @author russell
 * @param <T>
 * 
 */
public class FetchIndex<T> implements RiakOperation<List<String>> {

    private final RawClient client;
    protected final RiakIndex<T> index;
    private final String bucket;
    protected T value;
    protected T from;
    protected T to;
    protected boolean returnTerms;
    protected Integer maxResults;
    protected String continuation;

    private Retrier retrier;

    /**
     * @param index
     */
    public FetchIndex(RawClient client, String bucket, RiakIndex<T> index, Retrier retrier) {
        this.client = client;
        this.bucket = bucket;
        this.index = index;
        this.retrier = retrier;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.operations.RiakOperation#execute()
     */
    public List<String> execute() throws RiakException {
        if (value == null && (to == null || from == null)) {
            throw new IllegalStateException("Must set either value or range");
        }
        final IndexQuery indexQuery = makeIndexQuery();

        List<String> keys = retrier.attempt(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                return client.fetchIndex(indexQuery);
            }

        });

        return keys;
    }

    /**
     * Performs an index query as a streaming operation from Riak
     * 
     * Note that you must call {@link StreamingOperation#cancel() } on the returned 
     * StreamingOperation if you do not iterate through the entire result set.
     * 
     * <b>Note this is only available using Riak 1.4</b>
     * @return A StreamingOperation
     * @throws RiakException 
     */
    public StreamingOperation<IndexEntry> executeStreaming() throws RiakException  {
        if (value == null && (to == null || from == null)) {
            throw new IllegalStateException("Must set either value or range");
        }
        
        IndexSpec.Builder builder = makeIndexSpecBuilder();
        
        builder.withMaxResults(maxResults)
                .withReturnKeyAndIndex(returnTerms)
                .withContinuation(continuation);
        
        try {
            return client.fetchIndex(builder.build());
        } catch (IOException ex) {
            throw new RiakException(ex);
        }
        
                
    }
    
    private IndexSpec.Builder makeIndexSpecBuilder() {
        if (isRange()) {
            return makeRangeSpecBuilder();
        } else {
            return makeValueSpecBuilder();
        }
    }
    
    private IndexSpec.Builder makeRangeSpecBuilder() {
        if (to.getClass().equals(String.class)) {
            return new IndexSpec.Builder(bucket, index.getFullname())
                                    .withRangeStart((String)from)
                                    .withRangeEnd((String)to);
        } else if (Number.class.isAssignableFrom(to.getClass())) {
            return new IndexSpec.Builder(bucket, index.getFullname())
                                    .withRangeStart(((Number)from).longValue())
                                    .withRangeEnd(((Number)to).longValue());
        }
        
        return null;
    }
    
    private IndexSpec.Builder makeValueSpecBuilder() {
        if (value.getClass().equals(String.class)) {
            return new IndexSpec.Builder(bucket, index.getFullname())
                                    .withIndexKey((String)value);
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            return new IndexSpec.Builder(bucket, index.getFullname())
                                    .withIndexKey(((Number)value).longValue());
        }
        return null;
    }
    
    /**
     * @return
     */
    private IndexQuery makeIndexQuery() {
        // TODO move this to factory method on Abstract Query types
        if (isRange()) {
            return makeRangeQuery();
        } else {
            return makeValueQuery();
        }
    }

    private boolean isRange() {
        return to != null && from != null;
    }

    /**
     * @return
     */
    private IndexQuery makeValueQuery() {
        if (value.getClass().equals(String.class)) {
            return new BinValueQuery((BinIndex) index, bucket, (String) value);
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            return new IntValueQuery((IntIndex) index, bucket, ((Number)value).longValue());
        }
        return null;
    }

    /**
     * @return
     */
    private IndexQuery makeRangeQuery() {
        if (to.getClass().equals(String.class)) {
            return new BinRangeQuery((BinIndex) index, bucket, (String) from, (String) to);
        } else if (Number.class.isAssignableFrom(to.getClass())) {
            return new IntRangeQuery((IntIndex) index, bucket, 
                                     ((Number)from).longValue(), ((Number) to).longValue());
        } else {
            throw new RuntimeException("Unkown range query type " + to.getClass());
        }
    }

    public FetchIndex<T> withValue(T value) {
        this.value = value;
        return this;
    }

    public FetchIndex<T> from(T from) {
        this.from = from;
        return this;
    }

    public FetchIndex<T> to(T to) {
        this.to = to;
        return this;
    }
    
    /**
     * Pagination support for 2i queries. Will return {@code maxResults} entries 
     * starting at the lower range or the continuation returned from a previous query
     * 
     * <b>This is only available in v1.4+ of Riak and only via streaming</b>
     * @see FetchIndex#executeStreaming() 
     * 
     * @param maxResults
     * @return this
     */
    public FetchIndex<T> maxResults(int maxResults) {
        this.maxResults = maxResults;
        return this;
    }
    
    /**
     * Return both the object keys and the index values
     * 
     * * <b>This is only available in v1.4+ of Riak and only via streaming</b>
     * @see FetchIndex#executeStreaming() 
     * 
     * @param returnBoth
     * @return this
     */
    public FetchIndex<T> returnKeyAndIndexValue(boolean returnBoth) {
        this.returnTerms = returnBoth;
        return this;
    }
    
    /**
     * Sets the continuation received with a previous call
     * 
     * * * <b>This is only available in v1.4+ of Riak and only via streaming</b>
     * @see FetchIndex#executeStreaming() 
     * 
     * @see StreamingOperation#getContinuation() 
     * @param continuation
     * @return this
     */
    public FetchIndex<T> withContinuation(String continuation) {
        this.continuation = continuation;
        return this;
    }
}
