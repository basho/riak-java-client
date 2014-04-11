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

import com.basho.riak.client.operations.datatypes.DatatypeUpdate;
import com.basho.riak.client.operations.datatypes.Context;
import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakDatatype;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.Map;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public abstract class UpdateDatatype<T extends RiakDatatype,S,U> extends RiakCommand<S,U>
{

    protected final Location loc;
    private final Context ctx;
    private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

    @SuppressWarnings("unchecked")
    UpdateDatatype(Builder builder)
    {
        this.loc = builder.loc;
        this.ctx = builder.ctx;
	    this.options.putAll(builder.options);
    }
    
    protected final DtUpdateOperation buildCoreOperation(DatatypeUpdate update)
    {
        DtUpdateOperation.Builder builder = new DtUpdateOperation.Builder(loc);

        if (ctx != null)
        {
            builder.withContext(BinaryValue.create(ctx.getBytes()));
        }

        builder.withOp(update.getOp());

        for (Map.Entry<Option<?>, Object> entry : options.entrySet())
        {
            if (entry.getKey() == Option.DW)
            {
                builder.withDw(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == Option.N_VAL)
            {
                builder.withNVal((Integer) entry.getValue());
            }
            else if (entry.getKey() == Option.PW)
            {
                builder.withPw(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == Option.RETURN_BODY)
            {
                builder.withReturnBody((Boolean) entry.getValue());
            }
            else if (entry.getKey() == Option.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == Option.TIMEOUT)
            {
                builder.withTimeout((Integer) entry.getValue());
            }
            else if (entry.getKey() == Option.W)
            {
                builder.withW(((Quorum) entry.getValue()).getIntValue());
            }
        }

        return builder.build();

    }

    /**
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    */
   public static final class Option<T> extends RiakOption<T>
   {

       public static final Option<Quorum> DW = new Option<Quorum>("DW");
       public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");
       public static final Option<Quorum> PW = new Option<Quorum>("PW");
       public static final Option<Boolean> RETURN_BODY = new Option<Boolean>("RETURN_BODY");
       public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
       public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");
       public static final Option<Quorum> W = new Option<Quorum>("W");

       public Option(String name)
       {
           super(name);
       }
   }
    
    static abstract class Builder<T extends Builder<T>>
	{
		private final Location loc;
		private Context ctx;
		private Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

		Builder(Location location)
		{
			if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null.");
            }
            this.loc = location;
		}

        public T withContext(Context context)
		{
			if (context == null)
            {
                throw new IllegalArgumentException("Context cannot be null.");
            }
            this.ctx = context;
			return self();
		}

		public <U> T withOption(Option<U> option, U value)
		{
			this.options.put(option, value);
			return self();
		}

        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public T withTimeout(int timeout)
        {
            withOption(Option.TIMEOUT, timeout);
            return self();
        }
        
        protected abstract T self();
        protected abstract UpdateDatatype build();
    }

    static abstract class Response<T>
    {
        private final T datatype;
        private final Context context;
        private final BinaryValue generatedKey;

        Response(Context context, T datatype, BinaryValue generatedKey)
        {
            this.datatype = datatype;
            this.context = context;
            this.generatedKey = generatedKey;
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
        
        public boolean hasGeneratedKey()
        {
            return generatedKey != null;
        }
        
        public BinaryValue getGeneratedKey()
        {
            return generatedKey;
        }
    }
}
