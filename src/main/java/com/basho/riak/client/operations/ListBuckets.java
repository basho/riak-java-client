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
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.operations.Location.bucketType;
import static com.basho.riak.client.operations.Location.defaultBucketType;

public class ListBuckets extends RiakCommand<ListBuckets.Response>
{

    private final int timeout;
    private final BucketType type;

    public ListBuckets(BucketType type, int timeout)
    {
        this.type = type;
        this.timeout = timeout;
    }

    public ListBuckets()
    {
        this(defaultBucketType());
    }

    public ListBuckets(int timeout)
    {
        this(defaultBucketType(), timeout);
    }

    public ListBuckets(BucketType type)
    {
        this(type, -1);
    }

    @Override
    Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        ListBucketsOperation.Builder builder = new ListBucketsOperation.Builder();
        if (timeout > 0)
        {
            builder.withTimeout(timeout);
        }
        builder.withBucketType(type.getType());
        ListBucketsOperation operation = builder.build();
        cluster.execute(operation);
        return new Response(type, operation.get());
    }

    public static class Response implements Iterable<Bucket> {

        private final BucketType type;
        private final List<ByteArrayWrapper> buckets;

        public Response(BucketType type, List<ByteArrayWrapper> buckets)
        {
            this.type = type;
            this.buckets = buckets;
        }

        @Override
        public Iterator<Bucket> iterator()
        {
            return new Itr(buckets.iterator(), type);
        }
    }

    private static class Itr implements Iterator<Bucket>
    {
        private final Iterator<ByteArrayWrapper> iterator;
        private final BucketType type;

        private Itr(Iterator<ByteArrayWrapper> iterator, BucketType type)
        {
            this.iterator = iterator;
            this.type = type;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public Bucket next()
        {
            ByteArrayWrapper bucket = iterator.next();
            return Location.bucket(type, bucket);
        }

        @Override
        public void remove()
        {
            iterator.remove();
        }
    }

}
