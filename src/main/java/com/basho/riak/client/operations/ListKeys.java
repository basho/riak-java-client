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
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ListKeys extends RiakCommand<ListKeys.Response>
{

    private final Bucket bucket;
    private final int timeout;

    public ListKeys(Bucket bucket, int timeout)
    {
        this.bucket = bucket;
        this.timeout = timeout;
    }

    public ListKeys(Bucket bucket)
    {
        this(bucket, -1);
    }

    @Override
    Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        ListKeysOperation.Builder builder = new ListKeysOperation.Builder(bucket.getBucket());
        if (timeout > 0)
        {
            builder.withTimeout(timeout);
        }
        builder.withBucketType(bucket.getType());
        ListKeysOperation operation = builder.build();
        cluster.execute(operation);
        return new Response(bucket, operation.get());
    }

    public static class Response implements Iterable<Key>
    {

        private final Bucket bucket;
        private final List<ByteArrayWrapper> keys;

        public Response(Bucket bucket, List<ByteArrayWrapper> keys)
        {
            this.bucket = bucket;
            this.keys = keys;
        }

        @Override
        public Iterator<Key> iterator()
        {
            return new Itr(bucket, keys.iterator());
        }
    }

    private static class Itr implements Iterator<Key>
    {
        private final Iterator<ByteArrayWrapper> iterator;
        private final Bucket bucket;

        private Itr( Bucket bucket, Iterator<ByteArrayWrapper> iterator)
        {
            this.iterator = iterator;
            this.bucket = bucket;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public Key next()
        {
            ByteArrayWrapper key = iterator.next();
            return Location.key(bucket, key);
        }

        @Override
        public void remove()
        {
            iterator.remove();
        }
    }

}
