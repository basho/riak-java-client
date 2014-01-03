/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.unmodifiableList;

public class FetchIndex<T> extends RiakCommand<FetchIndex.Response<T>>
{

    private final Location bucket;
    private final Criteria op;
    private final Map<IndexOption<?>, Object> options = new HashMap<IndexOption<?>, Object>();
    private final Index<T> index;
    private final ByteArrayWrapper continuation;

    FetchIndex(Builder<T> builder)
    {
        this.bucket = builder.bucket;
        this.op = builder.op;
        this.index = builder.index;
        this.continuation = builder.continuation;
    }

    @Override
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        ByteArrayWrapper indexName = ByteArrayWrapper.create(index.getFullName());

        SecondaryIndexQueryOperation.Builder builder =
            new SecondaryIndexQueryOperation.Builder(bucket.getBucket(), indexName);

        builder.withBucketType(bucket.getType());

        for (Map.Entry<IndexOption<?>, Object> option : options.entrySet())
        {
            if (option.getKey() == IndexOption.MAX_RESULTS)
            {
                builder.withMaxResults((Integer) option.getValue());
            }
            else if (option.getKey() == IndexOption.RETURN_TERMS)
            {
                builder.withReturnKeyAndIndex((Boolean) option.getValue());
            }
        }

        op.configure(builder);

        if (continuation != null)
        {
            builder.withContinuation(continuation);
        }

        SecondaryIndexQueryOperation operation = builder.build();
        cluster.execute(operation);

        SecondaryIndexQueryOperation.Response opResponse = operation.get();

        ArrayList<IndexEntry<T>> indexEntries = new ArrayList<IndexEntry<T>>(opResponse.getEntryList().size());

        for (SecondaryIndexQueryOperation.Response.Entry entry : opResponse.getEntryList())
        {
            Location key = new Location(bucket.getBucket(), entry.getIndexKey()).withType(bucket.getType());
            T objectKey = index.convert(entry.getObjectKey());
            IndexEntry<T> indexEntry = new IndexEntry<T>(key, objectKey);
            indexEntries.add(indexEntry);
        }

        byte[] continuation = null;
        if (opResponse.hasContinuation())
        {
            continuation = opResponse.getContinuation().getValue();
        }

        return new Response<T>(continuation, indexEntries);
    }

    public static Criteria range(int start, int end)
    {
        return new RangeCriteria(start, end);
    }

    public static Criteria match(String term)
    {
        return new MatchCriteria(term);
    }

    public static Criteria match(int term)
    {
        return new MatchCriteria(term);
    }

    public static Criteria match(byte[] term)
    {
        return new MatchCriteria(term);
    }

    public static abstract class Criteria
    {
        abstract void configure(SecondaryIndexQueryOperation.Builder op);
    }

    private static class MatchCriteria extends Criteria
    {
        final ByteArrayWrapper match;

        public MatchCriteria(String match)
        {
            this.match = ByteArrayWrapper.create(match);
        }

        public MatchCriteria(int match)
        {
            this.match = ByteArrayWrapper.create(Integer.toString(match));
        }

        public MatchCriteria(byte[] match)
        {
            this.match = ByteArrayWrapper.create(match);
        }

        @Override
        void configure(SecondaryIndexQueryOperation.Builder op)
        {
            op.withIndexKey(match);
        }
    }

    private static class RangeCriteria extends Criteria
    {
        final int start;
        final int stop;

        public RangeCriteria(int start, int stop)
        {
            this.start = start;
            this.stop = stop;
        }

        @Override
        void configure(SecondaryIndexQueryOperation.Builder op)
        {
            op.withRangeStart(ByteArrayWrapper.create(Integer.toString(start)));
            op.withRangeEnd(ByteArrayWrapper.create(Integer.toString(stop)));
        }
    }

    public static final class IndexEntry<T>
    {
        private final Location key;
        private final T term;

        IndexEntry(Location key, T term)
        {
            this.key = key;
            this.term = term;
        }

        public Location getKey()
        {
            return key;
        }

        public boolean hasTerm()
        {
            return term != null;
        }

        public T getTerm()
        {
            return term;
        }
    }

    public static final class Response<T> implements Iterable<IndexEntry<T>>
    {

	    private final byte[] continuation;
        private final List<IndexEntry<T>> entries;

        Response(byte[] continuation, List<IndexEntry<T>> entries)
        {
	        this.continuation = continuation;
            this.entries = entries;
        }

        public boolean hasContinuation()
        {
            return continuation != null;
        }

        public byte[] getContinuation()
        {
            return continuation;
        }

        @Override
        public Iterator<IndexEntry<T>> iterator()
        {
            return unmodifiableList(entries).iterator();
        }
    }

	public static class Builder<T>
	{

		private final Location bucket;
		private final Map<IndexOption<?>, Object> options = new HashMap<IndexOption<?>, Object>();
		private final Index<T> index;
		private Criteria op;
		private ByteArrayWrapper continuation;

		public Builder(Location bucket, Index<T> index)
		{
			this.bucket = bucket;
			this.index = index;
		}

		public Builder<T> withCriteria(Criteria op)
		{
			this.op = op;
			return this;
		}

		public <U> Builder<T> withOption(IndexOption<U> option, U value)
		{
			this.options.put(option, value);
			return this;
		}

		public Builder<T> withContinuation(byte[] continuation)
		{
			this.continuation = ByteArrayWrapper.create(continuation);
			return this;
		}

		public FetchIndex<T> build()
		{
			return new FetchIndex<T>(this);
		}

	}

}

