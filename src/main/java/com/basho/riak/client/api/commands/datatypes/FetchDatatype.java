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
package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.operations.DtFetchOperation;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;

import java.util.HashMap;
import java.util.Map;

 /**
  * Base abstract class for all CRDT fetch commands.
  * @author Dave Rusek <drusek at basho dot com>
  * @since 2.0
  */
public abstract class FetchDatatype<T extends RiakDatatype,S,U> extends RiakCommand<S,U>
{
    private final Location location;
    private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

    public Location getLocation()
    {
       return location;
    }

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
     * Tuning parameters for all datatype fetch commands.
     * @author Dave Rusek <drusek at basho dot com>
     * @since 2.0
     */
    public static final class Option<T> extends RiakOption<T>
    {

        /**
         * Read Quorum.
         * How many replicas need to agree when fetching the object.
         */
        public static final Option<Quorum> R = new Option<Quorum>("R");

        /**
         * Primary Read Quorum.
         * How many primary replicas need to be available when retrieving the object.
         */
        public static final Option<Quorum> PR = new Option<Quorum>("PR");

        /**
         * Basic Quorum.
         * Whether to return early in some failure cases (eg. when r=1 and you get
         * 2 errors and a success basic_quorum=true would return an error)
         */
        public static final Option<Boolean> BASIC_QUORUM = new Option<Boolean>("BASIC_QUORUM");

        /**
         * Not Found OK.
         * Whether to treat notfounds as successful reads for the purposes of R
         */
        public static final Option<Boolean> NOTFOUND_OK = new Option<Boolean>("NOTFOUND_OK");

        /**
         * Timeout.
         * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
         */
        public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");
        public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
        public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");

        /**
         * Whether to return a context.
         * The default is true. Use this option if you're planning on only reading the datatype.
         */
        public static final Option<Boolean> INCLUDE_CONTEXT = new Option<Boolean>("INCLUDE_CONTEXT");

        public Option(String name)
        {
            super(name);
        }
    }

    /**
     * Base abstract builder for all datatype fetch command builders.
     * @param <T>
     */
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

        /**
         * Add an optional setting for this command.
         * This will be passed along with the request to Riak to tell it how
         * to behave when servicing the request.
         *
         * @param option the option
         * @param value the value for the option
         * @return a reference to this object.
         * @see Option
         */
        public <U> T withOption(Option<U> option, U value)
        {
            this.options.put(option, value);
            return self();
        }

        protected abstract T self();
    }

    /**
     * Base response for all CRDT fetch commands.
     * @param <T>
     */
    public static class Response<T extends RiakDatatype>
    {
        private final T datatype;
        private final Context context;

        protected Response(T datatype, Context context)
        {
            this.datatype = datatype;
            this.context = context;
        }

        /**
         * Get the datatype from this response.
         * @return the fetched datatype.
         */
        public T getDatatype()
        {
            return datatype;
        }

        /**
         * Check to see if a context is present in this response.
         * @return true if a context is present, false otherwise.
         */
        public boolean hasContext()
        {
            return context != null;
        }

        /**
         * Get the context from this response.
         * <p>
         * The context is used when a subsequent update to the datatype
         * is performed.
         * </p>
         * @return the context if present, null otherwise.
         */
        public Context getContext()
        {
            return context;
        }
    }
}
