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

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.operations.datatypes.Context;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakDatatype;

import java.util.HashMap;
import java.util.Map;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public abstract class FetchDatatype<T extends RiakDatatype,S,U> extends RiakCommand<S,U>
{

    private final Location location;
    private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

	@SuppressWarnings("unchecked")
    protected FetchDatatype(Builder builder)
    {
        this.location = builder.location;
	    this.options.putAll(builder.options);
    }

    public <V> FetchDatatype<T,S,U> withOption(Option<V> option, V value)
    {
        options.put(option, value);
        return this;
    }

	public abstract T extractDatatype(RiakDatatype element);

    protected final DtFetchOperation buildCoreOperation()
    {
        DtFetchOperation.Builder builder = 
            new DtFetchOperation.Builder(location);
        
        for (Map.Entry<Option<?>, Object> entry : options.entrySet())
        {
            if (entry.getKey() == Option.R)
            {
                builder.withR(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == Option.PR)
            {
                builder.withPr(((Quorum) entry.getValue()).getIntValue());
            }
            else if (entry.getKey() == Option.BASIC_QUORUM)
            {
                builder.withBasicQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == Option.NOTFOUND_OK)
            {
                builder.withNotFoundOK((Boolean) entry.getValue());
            }
            else if (entry.getKey() == Option.TIMEOUT)
            {
                builder.withTimeout((Integer) entry.getValue());
            }
            else if (entry.getKey() == Option.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) entry.getValue());
            }
            else if (entry.getKey() == Option.N_VAL)
            {
                builder.withNVal((Integer) entry.getValue());
            }
            else if (entry.getKey() == Option.INCLUDE_CONTEXT)
            {
                builder.includeContext((Boolean) entry.getValue());
            }
        }


        return builder.build();

    }
    
    /**
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    */
   public static final class Option<T> extends RiakOption<T> {

     public static final Option<Quorum> R = new Option<Quorum>("R");
     public static final Option<Quorum> PR = new Option<Quorum>("PR");
     public static final Option<Boolean> BASIC_QUORUM = new Option<Boolean>("BASIC_QUORUM");
     public static final Option<Boolean> NOTFOUND_OK = new Option<Boolean>("NOTFOUND_OK");
     public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");
     public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
     public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");
     public static final Option<Boolean> INCLUDE_CONTEXT = new Option<Boolean>("INCLUDE_CONTEXT");

     public Option(String name) {
       super(name);
     }
   }
	protected static abstract class Builder<T extends Builder<T>>
	{

		private final Location location;
		private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

		protected Builder(Location location)
		{
			if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            this.location = location;
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
            return withOption(Option.TIMEOUT, timeout);
        }
        
		public <U> T withOption(Option<U> option, U value)
		{
			this.options.put(option, value);
			return self();
		}

		protected abstract T self();

	}

    public static class Response<T extends RiakDatatype>
    {

        private final T datatype;
        private final Context context;

        protected Response(T datatype, Context context)
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

        public Context getContext()
        {
            return context;
        }
    }



}
