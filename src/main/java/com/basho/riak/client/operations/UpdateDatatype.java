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
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.operations.datatypes.*;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class UpdateDatatype<T extends RiakDatatype> extends RiakCommand<UpdateDatatype.Response<T>>
{

    private final Location loc;
    private final Context ctx;
    private final DatatypeUpdate<T> datatype;
    private final Map<DtUpdateOption<?>, Object> options = new HashMap<DtUpdateOption<?>, Object>();

    private UpdateDatatype(Builder<T> builder)
    {
        this.loc = builder.loc;
        this.ctx = builder.ctx;
        this.datatype = builder.datatype;
	    this.options.putAll(builder.options);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        DtUpdateOperation.Builder builder = new DtUpdateOperation.Builder(loc);

        if (ctx != null)
        {
            builder.withContext(BinaryValue.create(ctx.getBytes()));
        }

        builder.withOp(datatype.getOp());

        for (Map.Entry<DtUpdateOption<?>, Object> entry : options.entrySet())
        {
            if (entry.getKey() == DtUpdateOption.DW)
            {
                builder.withDw(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == DtUpdateOption.N_VAL)
            {
                builder.withNVal((Integer) entry.getValue());
            }
            else if (entry.getKey() == DtUpdateOption.PW)
            {
                builder.withPw(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == DtUpdateOption.RETURN_BODY)
            {
                builder.withReturnBody((Boolean) entry.getValue());
            }
            else if (entry.getKey() == DtUpdateOption.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == DtUpdateOption.TIMEOUT)
            {
                builder.withTimeout((Integer) entry.getValue());
            }
            else if (entry.getKey() == DtUpdateOption.W)
            {
                builder.withW(((Quorum) entry.getValue()).getIntValue());
            }
        }

        DtUpdateOperation operation = builder.build();
        DtUpdateOperation.Response crdtResponse = cluster.execute(operation).get();
        CrdtElement element = crdtResponse.getCrdtElement();

        T riakDatatype = null;
        if (element.isMap())
        {
            riakDatatype = (T) new RiakMap(element.getAsMap());
        }
        else if (element.isSet())
        {
            riakDatatype = (T) new RiakSet(element.getAsSet());
        }
        else if (element.isCounter())
        {
            riakDatatype = (T) new RiakCounter(element.getAsCounter());
        }

	    BinaryValue returnedKey = crdtResponse.hasGeneratedKey()
		    ? crdtResponse.getGeneratedKey()
		    : loc.getKey();

        Location key = new Location(loc.getBucketName()).setKey(returnedKey).setBucketType(loc.getBucketType());
        Context returnedCtx = new Context(crdtResponse.getContext().getValue());

        return new Response<T>(key, returnedCtx, riakDatatype);

    }

	public static class Builder<T extends RiakDatatype>
	{
		private final Location loc;
		private Context ctx;
		private DatatypeUpdate<T> datatype;
		private Map<DtUpdateOption<?>, Object> options = new HashMap<DtUpdateOption<?>, Object>();

		public Builder(Location loc)
		{
			this.loc = loc;
		}

		public Builder<T> withContext(Context context)
		{
			this.ctx = context;
			return this;
		}

		public <U> Builder<T> withOption(DtUpdateOption<U> option, U value)
		{
			this.options.put(option, value);
			return this;
		}

		public Builder<T> withUpdate(DatatypeUpdate<T> update)
		{
			this.datatype = update;
			return this;
		}

		public UpdateDatatype<T> build()
		{
			return new UpdateDatatype<T>(this);
		}
	}

    public static class Response<T>
    {
        private final T datatype;
        private final Context context;
        private final Location key;

        Response(Location key, Context context, T datatype)
        {
            this.key = key;
            this.datatype = datatype;
            this.context = context;
        }

        public Location getKey()
        {
            return key;
        }

        public boolean hasContext()
        {
            return context != null;
        }

        public Context getContext()
        {
            return context;
        }

        public boolean hasDatatype()
        {
            return datatype != null;
        }

        public T getDatatype()
        {
            return datatype;
        }
    }
}
