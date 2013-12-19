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

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.operations.datatypes.*;
import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FetchDatatype<T extends RiakDatatype> extends RiakCommand<FetchDatatype.Response<T>>
{

    private final Key key;
    private final DatatypeConverter<T> converter;
    private final Map<DtFetchOption<?>, Object> options = new HashMap<DtFetchOption<?>, Object>();

    public FetchDatatype(Key key, DatatypeConverter<T> converter)
    {
        this.key = key;
        this.converter = converter;
    }

    public static FetchDatatype<RiakMap> fetchMap(Key key)
    {
        return new FetchDatatype<RiakMap>(key, DatatypeConverter.asMap());
    }

    public static FetchDatatype<RiakSet> fetchSet(Key key)
    {
        return new FetchDatatype<RiakSet>(key, DatatypeConverter.asSet());
    }

    public static FetchDatatype<RiakCounter> fetchCounter(Key key)
    {
        return new FetchDatatype<RiakCounter>(key, DatatypeConverter.asCounter());
    }

    public <U> FetchDatatype<T> withOption(DtFetchOption<U> option, U value)
    {
        options.put(option, value);
        return this;
    }

    @Override
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        ByteArrayWrapper bucket = ByteArrayWrapper.create(key.getBucket().getValue());
        ByteArrayWrapper key = ByteArrayWrapper.create(this.key.getKey().getValue());
        DtFetchOperation.Builder builder = new DtFetchOperation.Builder(bucket, key);

        if (this.key.hasType())
        {
            ByteArrayWrapper type = ByteArrayWrapper.create(this.key.getType().getValue());
            builder.withBucketType(type);
        }

        for (Map.Entry<DtFetchOption<?>, Object> entry : options.entrySet())
        {
            if (entry.getKey() == DtFetchOption.R)
            {
                builder.withR(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == DtFetchOption.PR)
            {
                builder.withPr(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == DtFetchOption.BASIC_QUORUM)
            {
                builder.withBasicQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == DtFetchOption.NOTFOUND_OK)
            {
                builder.withNotFoundOK((Boolean) entry.getValue());
            }
            else if (entry.getKey() == DtFetchOption.TIMEOUT)
            {
                builder.withTimeout((Integer) entry.getValue());
            }
            else if (entry.getKey() == DtFetchOption.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == DtFetchOption.N_VAL)
            {
                builder.withNVal((Integer) entry.getValue());
            }
            else if (entry.getKey() == DtFetchOption.INCLUDE_CONTEXT)
            {
                builder.includeContext((Boolean) entry.getValue());
            }
        }

        DtFetchOperation operation = builder.build();
        cluster.execute(operation);

        DtFetchOperation.Response response = operation.get();
        CrdtElement element = response.getCrdtElement();
        ByteArrayWrapper context = response.getContext();

        T datatype = converter.convert(element);

        return new Response<T>(datatype, context.getValue());

    }

    public static class Response<T extends RiakDatatype>
    {

        private final T datatype;
        private final byte[] context;

        public Response(T datatype, byte[] context)
        {
            this.datatype = datatype;
            this.context = context;
        }

        public T getDatatype()
        {
            return datatype;
        }

        public boolean hasContext()
        {
            return context != null;
        }

        public byte[] getContext()
        {
            return context;
        }
    }

}
