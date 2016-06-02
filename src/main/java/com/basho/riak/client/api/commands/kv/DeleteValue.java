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
package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.query.Location;

import java.util.HashMap;
import java.util.Map;

/**
 * Command used to delete a value from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * Deleting an object from Riak is a simple matter of supplying a {@link com.basho.riak.client.core.query.Location}
 * and executing the operation.
 * </p>
 * <p>
 * Note that this operation returns a {@code Void} type upon success. Any failure
 * will be thrown as an exception when calling {@code DeleteValue} synchronously or
 * when calling the future's get methods. 
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * DeleteValue dv = new DeleteValue.Builder(loc).build();
 * client.execute(dv);}</pre>
 * </p>
 * <p>
 * All operations can called async as well.
 * <pre class="prettyprint">
 * {@code
 * ...
 * RiakFuture<Void, Location> future = client.executeAsync(dv);
 * ...
 * future.await();
 * if (future.isSuccess())
 * { 
 *     ... 
 * }}</pre>
 * </p>
 * 
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteValue extends RiakCommand<Void, Location>
{

    private final Location location;
    private final Map<Option<?>, Object> options =
        new HashMap<Option<?>, Object>();
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

        for (Map.Entry<Option<?>, Object> optPair : options.entrySet())
        {

            Option<?> option = optPair.getKey();

            if (option == Option.DW)
            {
                builder.withDw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == Option.N_VAL)
            {
                builder.withNVal((Integer) optPair.getValue());
            }
            else if (option == Option.PR)
            {
                builder.withPr(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == Option.R)
            {
                builder.withR(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == Option.PW)
            {
                builder.withPw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == Option.RW)
            {
                builder.withRw(((Quorum) optPair.getValue()).getIntValue());
            }
            else if (option == Option.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) optPair.getValue());
            }
            else if (option == Option.TIMEOUT)
            {
                builder.withTimeout((Integer) optPair.getValue());
            }
            else if (option == Option.W)
            {
                builder.withW(((Quorum) optPair.getValue()).getIntValue());
            }
        }

        return builder.build();
    }

    public final static class Option<T> extends RiakOption<T>
    {

        /**
         * Read Write Quorum.
         * Quorum for both operations (get and put) involved in deleting an object 
         */
        public static final Option<Quorum> RW = new Option<Quorum>("RW");
        /**
         * Read Quorum.
         * How many replicas need to agree when fetching the object.
         */
        public static final Option<Quorum> R = new Option<Quorum>("R");
        /**
         * Write Quorum.
         * How many replicas to write to before returning a successful response.
         */
        public static final Option<Quorum> W = new Option<Quorum>("W");
        /**
         * Primary Read Quorum.
         * How many primary replicas need to be available when retrieving the object.
         */
        public static final Option<Quorum> PR = new Option<Quorum>("PR");
        /**
         * Primary Write Quorum. 
         * How many primary nodes must be up when the write is attempted
         */
        public static final Option<Quorum> PW = new Option<Quorum>("PW");
        /**
         * Durable Write Quorum.
         * How many replicas to commit to durable storage before returning a successful response.
         */
        public static final Option<Quorum> DW = new Option<Quorum>("DW");
        /**
         * Timeout.
         * Sets the server-side timeout for this operation. The default is 60 seconds.
         */
        public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");
        public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
        public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");

        private Option(String name)
        {
            super(name);
        }
    }

    /**
     * Used to construct a DeleteValue command.
     */
    public static class Builder
    {

        private final Location location;
        private final Map<Option<?>, Object> options =
            new HashMap<Option<?>, Object>();
        private VClock vClock;

        /**
         * Construct a Builder for a DeleteValue command.
         * @param location the location of the object in Riak
         */
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
         * Add a delete option.
         *
         * @param option the option
         * @param value  the value associated with the option
         * @param <T>    the type required by the option
         * @return a reference to this object
         */
        public <T> Builder withOption(Option<T> option, T value)
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
            withOption(Option.TIMEOUT, timeout);
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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (location != null ? location.hashCode() : 0);
        result = prime * result + options.hashCode();
        result = prime * result + (vClock != null ? vClock.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof DeleteValue))
        {
            return false;
        }

        final DeleteValue other = (DeleteValue) obj;
        if (this.location != other.location && (this.location == null || !this.location.equals(other.location)))
        {
            return false;
        }
        if (this.options != other.options && (this.options == null || !this.options.equals(other.options)))
        {
            return false;
        }
        if (this.vClock != other.vClock && (this.vClock == null || !this.vClock.equals(other.vClock)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("{location: %s, options: %s, vClock: %s}",
                location, options, vClock);
    }
}
