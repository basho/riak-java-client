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

public class FetchIndex extends RiakCommand<FetchIndex.Response>
{

    private final Bucket bucket;
    private final String index;
    private final Criteria op;
    private final Map<IndexOption<?>, Object> options = new HashMap<IndexOption<?>, Object>();
    private ByteArrayWrapper continuation;

    public FetchIndex(Bucket bucket, String index, Criteria op)
    {
        this.bucket = bucket;
        this.index = index;
        this.op = op;
    }

    public static FetchIndex lookupIndex(Bucket bucket, String index, Criteria op)
    {
        return new FetchIndex(bucket, index, op);
    }

    public FetchIndex withContinuation(ByteArrayWrapper continuation)
    {
        this.continuation = continuation;
        return this;
    }

    public <T> FetchIndex withOption(IndexOption<T> option, T value)
    {
        options.put(option, value);
        return this;
    }

    @Override
    public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        return new Response(null, Collections.<IndexEntry<?>>emptyList());
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
        private final Key key;
        private final T value;

        IndexEntry(Key key, T value)
        {
            this.key = key;
            this.value = value;
        }

        public Key getKey()
        {
            return key;
        }

        public boolean hasValue()
        {
            return value != null;
        }

        public T getValue()
        {
            return value;
        }
    }

    public static final class Response implements Iterable<IndexEntry<?>>
    {

        private final ByteArrayWrapper continuation;
        private final List<IndexEntry<?>> entries;

        Response(ByteArrayWrapper continuation, List<IndexEntry<?>> entries)
        {
            this.continuation = continuation;
            this.entries = entries;
        }

        public boolean hasContinuation()
        {
            return continuation != null;
        }

        public ByteArrayWrapper getContinuation()
        {
            return continuation;
        }

        @Override
        public Iterator<IndexEntry<?>> iterator()
        {
            return unmodifiableList(entries).iterator();
        }
    }

}

