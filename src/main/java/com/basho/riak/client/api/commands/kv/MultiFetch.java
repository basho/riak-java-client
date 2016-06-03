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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.api.commands.ListenableFuture;
import com.basho.riak.client.api.commands.kv.FetchValue.Option;
import com.basho.riak.client.core.query.Location;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;


import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Command used to fetch multiple values from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * Riak itself does not support pipelining of requests. MutliFetch addresses this issue by using a thread to
 * parallelize and manage a set of async fetch operations for a given set of keys.
 * </p>
 * <p>
 * The result of executing this command is a {@code List} of {@link RiakFuture} objects, each one representing a single
 * fetch operation. The returned {@code RiakFuture} that contains that list completes
 * when all the FetchValue operations contained have finished.
 * <p/>
 * <pre class="prettyprint">
 * {@code
 * MultiFetch multifetch = ...;
 * MultiFetch.Response response = client.execute(multifetch);
 * List<MyPojo> myResults = new ArrayList<MyPojo>();
 * for (RiakFuture<FetchValue.Response, Location> f : response)
 * {
 *     try
 *     {
 *          FetchValue.Response response = f.get();
 *          myResults.add(response.getValue(MyPojo.class));
 *     }
 *     catch (ExecutionException e)
 *     {
 *         // log error, etc.
 *     }
 * }}</pre>
 * </p>
 * <p>
 * The maximum number of concurrent requests defaults to 10. This can be changed
 * when constructing the operation.
 * </p>
 * <p>
 * Be aware that because requests are being parallelized performance is also
 * dependent on the client's underlying connection pool. If there are no connections
 * available performance will suffer initially as connections will need to be established
 * or worse they could time out.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class MultiFetch extends RiakCommand<MultiFetch.Response, List<Location>>
{
    public static final int DEFAULT_MAX_IN_FLIGHT = 10;
    
    private final ArrayList<Location> locations = new ArrayList<Location>();
    private final Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();
    private final int maxInFlight;

    private MultiFetch(Builder builder)
    {
        this.locations.addAll(builder.keys);
        this.options.putAll(builder.options);
        this.maxInFlight = builder.maxInFlight;
    }

    @Override
    protected RiakFuture<Response, List<Location>> executeAsync(final RiakCluster cluster)
    {
        List<FetchValue> fetchOperations = buildFetchOperations();
        MultiFetchFuture future = new MultiFetchFuture(locations);
        
        Submitter submitter = new Submitter(fetchOperations, maxInFlight, 
                                            cluster, future);
        
        Thread t = new Thread(submitter);
        t.setDaemon(true);
        t.start();
        return future;
    }

    @SuppressWarnings("unchecked")
    private List<FetchValue> buildFetchOperations()
    {
        List<FetchValue> fetchValueOperations =
            new LinkedList<FetchValue>();
        
        for (Location location : locations)
        {
            FetchValue.Builder builder = new FetchValue.Builder(location);

            for (Option<?> option : options.keySet())
            {
                builder.withOption((Option<Object>) option, options.get(option));
            }

            fetchValueOperations.add(builder.build());
        }
        
        return fetchValueOperations;
        
    }
    
    /**
     * Used to construct a MutiFetch command.
     */
    public static class Builder
    {
        private ArrayList<Location> keys = new ArrayList<Location>();
        private Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();
        private int maxInFlight = DEFAULT_MAX_IN_FLIGHT;
        
        /**
         * Add a location to the list of locations to retrieve as part of
         * this multifetch operation.
         *
         * @param location the location to add.
         * @return this
         */
        public Builder addLocation(Location location)
        {
            keys.add(location);
            return this;
        }

        /**
         * Add a list of Locations to the list of locations to retrieve as part of
         * this multifetch operation.
         *
         * @param location a list of Locations
         * @return a reference to this object
         */
        public Builder addLocations(Location... location)
        {
            keys.addAll(Arrays.asList(location));
            return this;
        }

        /**
         * Add a set of keys to the list of Locations to retrieve as part of
         * this multifetch operation.
         *
         * @param location an Iterable set of Locations.
         * @return a reference to this object
         */
        public Builder addLocations(Iterable<Location> location)
        {
            for (Location loc : location)
            {
                keys.add(loc);
            }
            return this;
        }

        /**
         * Set the maximum number of requests to be in progress simultaneously.
         * <p>
         * As noted, Riak does not actually have "MultiFetch" functionality. This
         * operation simulates it by sending multiple fetch requests. This 
         * parameter controls how many outstanding requests are allowed simultaneously. 
         * </p>
         * @param maxInFlight the max number of outstanding requests.
         * @return a reference to this object.
         */
        public Builder withMaxInFlight(int maxInFlight)
        {
            this.maxInFlight = maxInFlight;
            return this;
        }
        
        /**
         * A {@link Option} to use with each fetch operation.
         *
         * @param option an option
         * @param value  the option's associated value
         * @param <U>    the type of the option's value
         * @return a reference to this object.
         */
        public <U> Builder withOption(Option<U> option, U value)
        {
            this.options.put(option, value);
            return this;
        }

        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for each fetch.
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
         * Build a {@link MultiFetch} operation from this builder
         *
         * @return an initialized {@link MultiFetch} operation
         */
        public MultiFetch build()
        {
            return new MultiFetch(this);
        }

    }

    /**
     * The response from Riak for a MultiFetch command.
     *
     */
    public static final class Response implements Iterable<RiakFuture<FetchValue.Response, Location>>
    {

        private final List<RiakFuture<FetchValue.Response, Location>> responses;

        Response(List<RiakFuture<FetchValue.Response, Location>> responses)
        {
            this.responses = responses;
        }

        @Override
        public Iterator<RiakFuture<FetchValue.Response, Location>> iterator()
        {
            return unmodifiableList(responses).iterator();
        }
        
        public List<RiakFuture<FetchValue.Response, Location>> getResponses()
        {
            return responses;
        }
        
    }

    private class Submitter implements Runnable, RiakFutureListener<FetchValue.Response, Location>
    {
        private final List<FetchValue> operations;
        private final Semaphore inFlight;
        private final AtomicInteger received = new AtomicInteger();
        private final RiakCluster cluster;
        private final MultiFetchFuture multiFuture;
        
        public Submitter(List<FetchValue> operations, int maxInFlight, 
                         RiakCluster cluster, MultiFetchFuture multiFuture)
        {
            this.operations = operations;
            this.cluster = cluster;
            this.multiFuture = multiFuture;
            inFlight = new Semaphore(maxInFlight);
        }
        
        @Override
        public void run()
        {
            for (FetchValue fv : operations)
            {
                try
                {
                    inFlight.acquire();
                }
                catch (InterruptedException ex)
                {
                    multiFuture.setFailed(ex);
                    break;
                }
                
                RiakFuture<FetchValue.Response, Location> future =
                    fv.executeAsync(cluster);
                future.addListener(this);
            }
        }

        @Override
        public void handle(RiakFuture<FetchValue.Response, Location> f)
        {
            multiFuture.addFetchFuture(f);
            inFlight.release();
            int completed = received.incrementAndGet();
            if (completed == operations.size())
            {
                multiFuture.setCompleted();
            }
        }
        
    }
    
    private class MultiFetchFuture extends ListenableFuture<Response, List<Location>>
    {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final List<Location> locations;
        private final List<RiakFuture<FetchValue.Response, Location>> futures;
        private volatile Throwable exception;
        
        private MultiFetchFuture(List<Location> locations)
        {
            this.locations = locations;
            futures = 
                Collections.synchronizedList(new LinkedList<RiakFuture<FetchValue.Response, Location>>());
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public Response get() throws InterruptedException
        {
            latch.await();
            return new Response(futures);
        }

        @Override
        public Response get(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
            if (isDone())
            {
                return new Response(futures);
            }
            else
            {
                return null;
            }
        }

        @Override
        public Response getNow()
        {
            if (isDone())
            {
                return new Response(futures);
            }
            else
            {
                return null;
            }
        }
        
        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return latch.getCount() != 1;
        }

        @Override
        public void await() throws InterruptedException
        {
            latch.await();
        }

        @Override
        public void await(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
        }

        @Override
        public boolean isSuccess()
        {
            return isDone() && exception == null;
        }

        @Override
        public List<Location> getQueryInfo()
        {
            return locations;
        }
        
        @Override
        public Throwable cause()
        {
            return exception;
        }
        
        private void addFetchFuture(RiakFuture<FetchValue.Response, Location> future)
        {
            futures.add(future);
        }
        
        private void setCompleted()
        {
            latch.countDown();
            notifyListeners();
        }
        
        private void setFailed(Throwable t)
        {
            this.exception = t;
            latch.countDown();
            notifyListeners();
        }
 
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + locations.hashCode();
        result = prime * result + options.hashCode();
        result = prime * result + maxInFlight;
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

        final MultiFetch other = (MultiFetch) obj;
        if (this.locations != other.locations && (this.locations == null || !this.locations.equals(other.locations)))
        {
            return false;
        }
        if (this.options != other.options && (this.options == null || !this.options.equals(other.options)))
        {
            return false;
        }
        if (this.maxInFlight != other.maxInFlight)
        {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString()
    {
        return String.format("{locations: %s, options: %s, maxInFlight: %s}",
                locations, options, maxInFlight);
    }
}
