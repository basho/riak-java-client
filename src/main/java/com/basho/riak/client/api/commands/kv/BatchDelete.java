package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.ListenableFuture;
import com.basho.riak.client.api.commands.kv.DeleteValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.query.Location;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;

/**
 * Command used to delete multiple values from Riak.
 * Shamelessly adapted from {@code MultiFetch}
 * <p>
 * Riak itself does not support pipelining of requests. BatchDelete addresses this issue by using a thread to
 * parallelize and manage a set of async delete operations for a given set of keys.
 * </p>
 * <p>
 * The result of executing this command is a {@code List} of {@link RiakFuture} objects, each one representing a single
 * delete operation. The returned {@code RiakFuture} that contains that list completes
 * when all the DeleteValue operations contained have finished.
 * <p/>
 * <pre class="prettyprint">
 * {@code
 * BatchDelete batchdelete = ...;
 * BatchDelete.Response response = client.execute(batchdelete);
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
 * @author Gerard Stannard
 *         gerards at tacklocal dot com
 * @since 3.0
 */
public final class BatchDelete extends RiakCommand<BatchDelete.Response, List<Location>>
{
    public static final int DEFAULT_MAX_IN_FLIGHT = 10;

    private final ArrayList<Location> locations = new ArrayList<>();
    private final Map<Option<?>, Object> options = new HashMap<>();
    private final int maxInFlight;

    private BatchDelete(Builder builder)
    {
        this.locations.addAll(builder.keys);
        this.options.putAll(builder.options);
        this.maxInFlight = builder.maxInFlight;
    }

    @Override
    protected RiakFuture<Response, List<Location>> executeAsync(final RiakCluster cluster)
    {
        List<DeleteValue> deleteOperations = buildDeleteOperations();
        BatchDeleteFuture future = new BatchDeleteFuture(locations);

        Submitter submitter = new Submitter(deleteOperations, maxInFlight, cluster, future);

        Thread t = new Thread(submitter);
        t.setDaemon(true);
        t.start();
        return future;
    }

    @SuppressWarnings("unchecked")
    private List<DeleteValue> buildDeleteOperations()
    {
        List<DeleteValue> deleteValueOperations = new LinkedList<>();

        for (Location location : locations)
        {
            DeleteValue.Builder builder = new DeleteValue.Builder(location);

            for (Option<?> option : options.keySet())
            {
                builder.withOption((Option<Object>) option, options.get(option));
            }

            deleteValueOperations.add(builder.build());
        }

        return deleteValueOperations;

    }

    /**
     * Used to construct a BatchDelete command.
     */
    public static class Builder
    {
        private ArrayList<Location> keys = new ArrayList<>();
        private Map<Option<?>, Object> options = new HashMap<>();
        private int maxInFlight = DEFAULT_MAX_IN_FLIGHT;

        /**
         * Add a location to the list of locations to delete as part of
         * this batchdelete operation.
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
         * Add a list of Locations to the list of locations to delete as part of
         * this batchdelete operation.
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
         * Add a set of keys to the list of Locations to delete as part of
         * this batchdelete operation.
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
         * As noted, Riak does not actually have "BatchDelete" functionality. This
         * operation simulates it by sending multiple delete requests. This
         * parameter controls how many outstanding requests are allowed simultaneously.
         * </p>
         *
         * @param maxInFlight the max number of outstanding requests.
         * @return a reference to this object.
         */
        public Builder withMaxInFlight(int maxInFlight)
        {
            this.maxInFlight = maxInFlight;
            return this;
        }

        /**
         * A {@link com.basho.riak.client.api.commands.kv.DeleteValue.Option} to use with each delete operation.
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
         * this value will override that default for each delete.
         * </p>
         *
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            withOption(Option.TIMEOUT, timeout);
            return this;
        }

        /**
         * Build a {@link BatchDelete} operation from this builder
         *
         * @return an initialized {@link BatchDelete} operation
         */
        public BatchDelete build()
        {
            return new BatchDelete(this);
        }

    }

    /**
     * The response from Riak for a BatchDelete command.
     */
    public static final class Response implements Iterable<RiakFuture<Void, Location>>
    {

        private final List<RiakFuture<Void, Location>> responses;

        Response(List<RiakFuture<Void, Location>> responses)
        {
            this.responses = responses;
        }

        @Override
        public Iterator<RiakFuture<Void, Location>> iterator()
        {
            return unmodifiableList(responses).iterator();
        }

        public List<RiakFuture<Void, Location>> getResponses()
        {
            return responses;
        }

    }

    private class Submitter implements Runnable, RiakFutureListener<Void, Location>
    {
        private final List<DeleteValue> operations;
        private final Semaphore inFlight;
        private final AtomicInteger received = new AtomicInteger();
        private final RiakCluster cluster;
        private final BatchDeleteFuture batchDeleteFuture;

        public Submitter(List<DeleteValue> operations,
                         int maxInFlight,
                         RiakCluster cluster,
                         BatchDeleteFuture batchDeleteFuture)
        {
            this.operations = operations;
            this.cluster = cluster;
            this.batchDeleteFuture = batchDeleteFuture;
            inFlight = new Semaphore(maxInFlight);
        }

        @Override
        public void run()
        {
            for (DeleteValue fv : operations)
            {
                try
                {
                    inFlight.acquire();
                }
                catch (InterruptedException ex)
                {
                    batchDeleteFuture.setFailed(ex);
                    break;
                }

                RiakFuture<Void, Location> future = fv.executeAsync(cluster);
                future.addListener(this);
            }
        }

        @Override
        public void handle(RiakFuture<Void, Location> f)
        {
            batchDeleteFuture.addDeleteFuture(f);
            inFlight.release();
            int completed = received.incrementAndGet();
            if (completed == operations.size())
            {
                batchDeleteFuture.setCompleted();
            }
        }

    }

    private class BatchDeleteFuture extends ListenableFuture<Response, List<Location>>
    {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final List<Location> locations;
        private final List<RiakFuture<Void, Location>> futures;
        private volatile Throwable exception;

        private BatchDeleteFuture(List<Location> locations)
        {
            this.locations = locations;
            futures = Collections.synchronizedList(new LinkedList<RiakFuture<Void, Location>>());
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

        private void addDeleteFuture(RiakFuture<Void, Location> future)
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
}
