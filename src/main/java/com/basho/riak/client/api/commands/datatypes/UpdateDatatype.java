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
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Base abstract class used for all datatype updates.
 * @author Dave Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class UpdateDatatype<T extends RiakDatatype,S,U> extends RiakCommand<S,U>
{

    protected final Namespace namespace;
    protected final BinaryValue key;
    private final Context ctx;
    private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

    @SuppressWarnings("unchecked")
    UpdateDatatype(Builder builder)
    {
        this.namespace = builder.namespace;
        this.key = builder.key;
        this.ctx = builder.ctx;
	    this.options.putAll(builder.options);
    }
    
    protected final DtUpdateOperation buildCoreOperation(DatatypeUpdate update)
    {
        DtUpdateOperation.Builder builder;
        
        if (key != null)
        {
            Location loc = new Location(namespace, key);
            builder = new DtUpdateOperation.Builder(loc);
        }
        else
        {
            builder = new DtUpdateOperation.Builder(namespace);
        }
        
        if (ctx != null)
        {
            builder.withContext(ctx.getValue());
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
    * Tuning parameters for all datatype fetch commands.
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    */
   public static final class Option<T> extends RiakOption<T>
   {

       /**
        * Durable Write Quorum.
        * How many replicas to commit to durable storage before returning a successful response.
        */
       public static final Option<Quorum> DW = new Option<Quorum>("DW");
       public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");
       /**
        * Primary Write Quorum.
        * How many primary nodes must be up when the write is attempted.
        */
       public static final Option<Quorum> PW = new Option<Quorum>("PW");
       /**
        * Return Body.
        * Return the object stored in Riak. Note this will return all siblings.
        */
       public static final Option<Boolean> RETURN_BODY = new Option<Boolean>("RETURN_BODY");
       public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
       /**
        * Timeout.
        * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
        */
       public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");
       /**
        * Write Quorum.
        * How many replicas to write to before returning a successful response.
        */
       public static final Option<Quorum> W = new Option<Quorum>("W");

       public Option(String name)
       {
           super(name);
       }
   }
    
   /**
    * Base abstract builder for all datatype update builders.
    */
    public static abstract class Builder<T extends Builder<T>>
	{
		private final Namespace namespace;
        private BinaryValue key;
		private Context ctx;
		private Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

        /**
         * Constructs a builder for a datatype update.
         * @param location the location of the datatype object in Riak.
         */
		Builder(Location location)
		{
			if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null.");
            }
            this.namespace = location.getNamespace();
            this.key = location.getKey();
		}

        /**
         * Constructs a builder for a datatype update with only a Namespace.
         * <p>
         * By providing only a Namespace with the update, Riak will create the 
         * datatype object, generate the key, 
         * and return it in the response. 
         * </p>
         * @param namespace the namespace to create the datatype.
         * @see Response#getGeneratedKey() 
         */
        Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null.");
            }
            this.namespace = namespace;
        }
        
        /**
         * Include the context from a previous fetch.
         * <p>
         * When updating a previously fetched set or map you generally 
         * want to include the context returned from that query with the update.
         * </p>
         * @param context the Context from a previous fetch.
         * @return a reference to this object.
         */
        public T withContext(Context context)
		{
			if (context == null)
            {
                throw new IllegalArgumentException("Context cannot be null.");
            }
            this.ctx = context;
			return self();
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
        
        /**
         * Return the updated datatype.
         * <p>
         * By default the datatype update commands are "fire and forget" in that
         * the modified datatype in Riak is not returned. Setting this to true
         * returns the modified datatype in the response.
         * </p>
         * @param returnDatatype true to return the modified datatype.
         * @return a reference to this object.
         */
        public T withReturnDatatype(boolean returnDatatype)
        {
            withOption(Option.RETURN_BODY, true);
            return self();
        }
        
        protected abstract T self();
        protected abstract UpdateDatatype build();
    }

    /**
     * Base abstract class used for all datatype update responses.
     */
    public static abstract class Response<T>
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

        /**
         * Check to see if this response includes a Context.
         * @return true if Context is present, false otherwise.
         */
        public boolean hasContext()
        {
            return context != null;
        }

        /**
         * Get the returned context.
         * @return the Context, or null if not present.
         */
        public Context getContext()
        {
            return context;
        }

        /**
         * Check to see if this resposne includes the updated datatype.
         * @return true if datatype is present, false otherwise.
         * @see Builder#withReturnDatatype(boolean) 
         */
        public boolean hasDatatype()
        {
            return datatype != null;
        }

        /**
         * Get the returned datatype.
         * @return the updated datatype, or null if not present.
         * @see Builder#withReturnDatatype(boolean) 
         */
        public T getDatatype()
        {
            return datatype;
        }
        
        /**
         * Check to see if the response includes a generated key.
         * <p>This will only be true if the datatype update was sent with
         * only a Namespace</p>
         * @return true if key is present, false otherwise.
         */
        public boolean hasGeneratedKey()
        {
            return generatedKey != null;
        }
        
        /**
         * Get the returned generated key.
         * @return the key, or null if not present.
         * @see #hasGeneratedKey() 
         */
        public BinaryValue getGeneratedKey()
        {
            return generatedKey;
        }
    }
}
