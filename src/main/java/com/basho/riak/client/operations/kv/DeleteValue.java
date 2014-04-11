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
package com.basho.riak.client.operations.kv;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.query.Location;

import java.util.HashMap;
import java.util.Map;

/**
 * Command used to delete a value from Riak.
 * <p>
 * Deleting an object from Riak is a simple matter of supplying a {@link com.basho.riak.client.query.Location}
 * and executing the DeleteValue operation.
 * <pre>
 * Location loc = new Location("my_bucket")..setBucketType("my_type").setKey("my_key");
 * DeleteValue dv = new DeleteValue.Builder(loc).build();
 * DeleteValue.Response resp = client.execute(dv);
 * </pre>
 * </p>
 * 
 * 
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteValue extends RiakCommand<Void, Location>
{

    private final Location location;
    private final Map<DeleteOption<?>, Object> options =
	    new HashMap<DeleteOption<?>, Object>();
    private final VClock vClock;

    public DeleteValue(Builder builder)
    {
        this.location = builder.location;
        this.options.putAll(builder.options);
        this.vClock = builder.vClock;
    }

    @Override
    protected RiakFuture<Void, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, Location> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Void, Location, Void, Location> future =
            new CoreFutureAdapter<Void, Location, Void, Location>(coreFuture)
            {
                @Override
                protected Void convertResponse(Void coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected Location convertQueryInfo(Location coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    private DeleteOperation buildCoreOperation()
    {
        DeleteOperation.Builder builder = new DeleteOperation.Builder(location);

        if (vClock != null)
        {
            builder.withVclock(vClock);
        }

        for (Map.Entry<DeleteOption<?>, Object> optPair : options.entrySet())
        {

            DeleteOption<?> option = optPair.getKey();

            if (option == DeleteOption.DW)
            {
                builder.withDw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == DeleteOption.N_VAL)
            {
                builder.withNVal((Integer) optPair.getValue());
            }
            else if (option == DeleteOption.PR)
            {
                builder.withPr(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == DeleteOption.R)
            {
                builder.withR(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == DeleteOption.PW)
            {
                builder.withPw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == DeleteOption.RW)
            {
                builder.withRw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == DeleteOption.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) optPair.getValue());
            }
            else if (option == DeleteOption.TIMEOUT)
            {
                builder.withTimeout((Integer) optPair.getValue());
            }
            else if (option == DeleteOption.W)
            {
                builder.withW(((Quorum) optPair.getValue()).getIntValue());
            }
        }

        return builder.build();
    }

    /**
     * The response from Riak for the DeleteValue command.
     */
    public static class Response
    {
        private final boolean deleted;

        protected Response(boolean deleted)
        {
            this.deleted = deleted;
        }

        /**
         * Indicates a successful deletion.
         * 
         * @return true, always. 
         */
        public boolean isDeleted()
        {
            return deleted;
        }
    }

    /**
     * Used to construct a DeleteValue command.
     */
	public static class Builder
	{

		private final Location location;
		private final Map<DeleteOption<?>, Object> options =
			new HashMap<DeleteOption<?>, Object>();
		private VClock vClock;

		public Builder(Location location)
		{
			if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            this.location = location;
		}

		/**
		 * Specify the VClock to use when deleting the object from Riak.
		 *
		 * @param vClock the vclock
		 * @return this
		 */
		public Builder withVClock(VClock vClock)
		{
			this.vClock = vClock;
			return this;
		}

		/**
		 * Add a delete option
		 *
		 * @param option the option
		 * @param value  the value associated with the option
		 * @param <T>    the type required by the option
		 * @return a reference to this object
		 */
		public <T> Builder withOption(DeleteOption<T> option, T value)
		{
			options.put(option, value);
			return this;
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
        public Builder withTimeout(int timeout)
        {
            withOption(DeleteOption.TIMEOUT, timeout);
            return this;
        }
        
        /**
         * Construct a DeleteValue object.
         * @return a new DeleteValue instance.
         */
		public DeleteValue build()
		{
			return new DeleteValue(this);
		}

	}

}
