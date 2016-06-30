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

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.query.Location;

import java.util.HashMap;
import java.util.Map;


/**
 * Command used to fetch a value from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * Fetching an object from Riak is a simple matter of supplying a {@link com.basho.riak.client.core.query.Location}
 * and executing the FetchValue operation.
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type","my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchValue fv = new FetchValue.Builder(loc).build();
 * FetchValue.Response response = client.execute(fv);
 * RiakObject obj = response.getValue(RiakObject.class);}</pre>
 * </p>
 * <p>
 * All operations can called async as well.
 * <pre class="prettyprint">
 * {@code
 * ...
 * RiakFuture<FetchValue.Response, Location> future = client.executeAsync(fv);
 * ...
 * future.await();
 * if (future.isSuccess())
 * { 
 *     ... 
 * }}</pre>
 * </p>
 * <p>
 * ORM features are also provided when retrieving the results from the response. 
 * By default, JSON serialization / deserializtion is used. For example, if 
 * the value stored in Riak was JSON and mapped to your class {@code MyPojo}:
 * <pre class="prettyprint">
 * {@code
 * ...
 * MyPojo mp = response.getValue(MyPojo.class);
 * ...}</pre>
 * </p>
 * 
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 * @see Response
 */
public final class FetchValue extends RiakCommand<FetchValue.Response, Location>
{

    private final Location location;
    private final Map<Option<?>, Object> options =
            new HashMap<Option<?>, Object>();

    FetchValue(Builder builder)
    {
        this.location = builder.location;
        this.options.putAll(builder.options);
    }

    @Override
    protected final RiakFuture<Response, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<FetchOperation.Response, Location> coreFuture = 
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Response, Location, FetchOperation.Response, Location> future = 
            new CoreFutureAdapter<Response, Location, FetchOperation.Response, Location>(coreFuture)
            {
                @Override
                protected Response convertResponse(FetchOperation.Response coreResponse)
                {
                    return new Response.Builder().withNotFound(coreResponse.isNotFound()) 
                                        .withUnchanged(coreResponse.isUnchanged())
                                        .withValues(coreResponse.getObjectList()) 
                                        .withLocation(location) // for ORM
                                        .build();
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

    private FetchOperation buildCoreOperation()
    {
        FetchOperation.Builder builder = new FetchOperation.Builder(location);

        for (Map.Entry<Option<?>, Object> opPair : options.entrySet())
        {

            RiakOption<?> option = opPair.getKey();

            if (option == Option.R)
            {
                builder.withR(((Quorum) opPair.getValue()).getIntValue());
            } else if (option == Option.DELETED_VCLOCK)
            {
                builder.withReturnDeletedVClock((Boolean) opPair.getValue());
            } else if (option == Option.TIMEOUT)
            {
                builder.withTimeout((Integer) opPair.getValue());
            } else if (option == Option.HEAD)
            {
                builder.withHeadOnly((Boolean) opPair.getValue());
            } else if (option == Option.BASIC_QUORUM)
            {
                builder.withBasicQuorum((Boolean) opPair.getValue());
            } else if (option == Option.IF_MODIFIED)
            {
                VClock clock = (VClock) opPair.getValue();
                builder.withIfNotModified(clock.getBytes());
            } else if (option == Option.N_VAL)
            {
                builder.withNVal((Integer) opPair.getValue());
            } else if (option == Option.PR)
            {
                builder.withPr(((Quorum) opPair.getValue()).getIntValue());
            } else if (option == Option.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) opPair.getValue());
            } else if (option == Option.NOTFOUND_OK)
            {
                builder.withNotFoundOK((Boolean) opPair.getValue());
            }

        }

        return builder.build();
    }

    /**
     * A response from Riak containing results from a FetchValue command.
     * <p>
     * The Response, unless marked not found or unchanged, will contain one or
     * more objects returned from Riak (all siblings are returned if present).
     * </p>
     */
    public static class Response extends KvResponseBase
    {
        private final boolean notFound;
        private final boolean unchanged;

        Response(Init<?> builder)
        {
            super(builder);
            this.notFound = builder.notFound;
            this.unchanged = builder.unchanged;
        }

        /**
         * Determine if there was a value in Riak.
         * <p>
         * If there was no value present at the supplied {@code Location} in
         * Riak, this will be true.
         * </p>
         *
         * @return true if there was no value in Riak.
         */
        public boolean isNotFound()
        {
            return notFound;
        }

        /**
         * Determine if the value is unchanged.
         * <p/>
         * If the fetch request set {@link com.basho.riak.client.api.commands.kv.FetchValue.Option#IF_MODIFIED}
         * this indicates if the value in Riak has been modified.
         * <p/>
         *
         * @return true if the vector clock for the object in Riak matched the
         * supplied vector clock, false otherwise.
         */
        public boolean isUnchanged()
        {
            return unchanged;
        }

        protected static abstract class Init<T extends Init<T>> extends KvResponseBase.Init<T>
        {
            private boolean notFound;
            private boolean unchanged;
            
            T withUnchanged(boolean unchanged)
            {
                this.unchanged = unchanged;
                return self();
            }
            
            T withNotFound(boolean notFound)
            {
                this.notFound = notFound;
                return self();
            }
        }
        
        static class Builder extends Init<Builder>
        {

            @Override
            protected Builder self()
            {
                return this;
            }

            @Override
            Response build()
            {
                return new Response(this);
            }
            
        }

    }

    /**
    * Options for controlling how Riak performs the fetch operation.
    * <p>
    * These options can be supplied to the {@link FetchValue.Builder} to change
    * how Riak performs the operation. These override the defaults provided
    * by the bucket.
    * </p>
    *
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">Replication Properties</a>
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
        * If Modified.
        * When a vector clock is supplied with this option, only return the object 
        * if the vector clocks don't match.
        */
       public static final Option<VClock> IF_MODIFIED = new Option<VClock>("IF_MODIFIED");
       /**
        * Head.
        * return the object with the value(s) set as empty. This allows you to get the 
        * meta data without a potentially large value. Analogous to an HTTP HEAD request.
        */
       public static final Option<Boolean> HEAD = new Option<Boolean>("HEAD");
       /**
        * Deleted VClock.
        * By default single tombstones are not returned by a fetch operations. This 
        * will return a Tombstone if it is present. 
        */
       public static final Option<Boolean> DELETED_VCLOCK = new Option<Boolean>("DELETED_VCLOCK");
       /**
        * Timeout.
        * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
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
     * Used to construct a FetchValue command. 
     */
    public static class Builder
    {

        private final Location location;
        private final Map<Option<?>, Object> options =
                new HashMap<Option<?>, Object>();

        /**
         * Constructs a builder for a FetchValue operation using the supplied location.
         * @param location the location of the object you want to fetch from Riak. 
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
         * Add an optional setting for this command.
         * This will be passed along with the request to Riak to tell it how
         * to behave when servicing the request.
         *
         * @param option the option
         * @param value the value for the option
         * @return a reference to this object.
         */
        public <U> Builder withOption(Option<U> option, U value)
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
         * Build a {@link FetchValue} object
         *
         * @return a FetchValue command
         */
        public FetchValue build()
        {
            return new FetchValue(this);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (location != null ? location.hashCode() : 0);;
        result = prime * result + options.hashCode();
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
        if (!(obj instanceof FetchValue))
        {
            return false;
        }

        final FetchValue other = (FetchValue) obj;
        if (this.location != other.location && (this.location == null || !this.location.equals(other.location)))
        {
            return false;
        }
        if (this.options != other.options && (this.options == null || !this.options.equals(other.options)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("{location: %s, options: %s}", location, options);
    }
}
