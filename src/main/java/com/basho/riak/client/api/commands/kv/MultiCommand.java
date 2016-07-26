package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.ListenableFuture;
import com.basho.riak.client.api.commands.RiakOption;
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


abstract class MultiCommand<BaseCommand extends RiakCommand<BaseResponseType, Location>,
                                   BaseBuilder extends KvBuilderBase<BaseCommand>,
                                   ResponseType extends Iterable<RiakFuture<BaseResponseType, Location>>,
                                   BaseResponseType>
        extends RiakCommand<ResponseType, List<Location>>
{
    public static final int DEFAULT_MAX_IN_FLIGHT = 10;

    protected final ArrayList<Location> locations;
    protected final Map<RiakOption<?>, Object> options = new HashMap<>();
    protected final int maxInFlight;

    @SuppressWarnings("unchecked")
    protected MultiCommand(Builder builder)
    {
        this.locations = builder.getKeys();
        this.options.putAll(builder.getOptions());
        this.maxInFlight = builder.maxInFlight;
    }


    @Override
    protected RiakFuture<ResponseType, List<Location>> executeAsync(final RiakCluster cluster)
    {
        List<BaseCommand> operations = buildOperations();
        MultiFuture future = new MultiFuture(locations);

        Submitter submitter = new Submitter(operations, maxInFlight, cluster, future);

        Thread t = new Thread(submitter);
        t.setDaemon(true);
        t.start();
        return future;
    }

    @SuppressWarnings("unchecked")
    List<BaseCommand> buildOperations()
    {
        List<BaseCommand> baseOperations = new LinkedList<>();

        for (Location location : locations)
        {
            BaseBuilder builder = createBaseBuilderType(location);

            for (RiakOption<?> option : options.keySet())
            {
                builder.addOption(option, options.get(option));
            }

            baseOperations.add(builder.build());
        }

        return baseOperations;

    }

    @Override
    public String toString()
    {
        return String.format("{locations: %s, options: %s, maxInFlight: %s}", locations, options, maxInFlight);
    }

    protected abstract ResponseType createResponseType(List<RiakFuture<BaseResponseType, Location>> futures);
    protected abstract BaseBuilder createBaseBuilderType(Location location);
    protected abstract RiakFuture<BaseResponseType, Location> executeBaseCommandAsync(BaseCommand command, RiakCluster cluster);

    protected static abstract class Builder<BuiltType, ConcreteBuilder extends Builder<BuiltType, ConcreteBuilder>>
    {
        private ArrayList<Location> keys = new ArrayList<Location>();
        private Map<RiakOption<?>, Object> options = new HashMap<>();
        private int maxInFlight = DEFAULT_MAX_IN_FLIGHT;

        public ArrayList<Location> getKeys()
        {
            return keys;
        }

        public Map<RiakOption<?>, Object> getOptions()
        {
            return options;
        }

        public int getMaxInFlight()
        {
            return maxInFlight;
        }

        /**
         * Add a location to the list of locations to interact with as part of
         * this operation.
         *
         * @param location the location to add.
         * @return this
         */
        public ConcreteBuilder addLocation(Location location)
        {
            keys.add(location);
            return self();
        }

        /**
         * Add a list of Locations to the list of locations to interact with as part of
         * this operation.
         *
         * @param location a list of Locations
         * @return a reference to this object
         */
        public ConcreteBuilder addLocations(Location... location)
        {
            keys.addAll(Arrays.asList(location));
            return self();
        }

        /**
         * Add a set of keys to the list of Locations to interact with as part of
         * this operation.
         *
         * @param location an Iterable set of Locations.
         * @return a reference to this object
         */
        public ConcreteBuilder addLocations(Iterable<Location> location)
        {
            for (Location loc : location)
            {
                keys.add(loc);
            }
            return self();
        }

        /**
         * Set the maximum number of requests to be in progress simultaneously.
         * <p>
         * As noted, Riak does not actually have any Batch or Multi operation functionality. This
         * operation simulates it by sending multiple requests. This
         * parameter controls how many outstanding requests are allowed simultaneously.
         * </p>
         *
         * @param maxInFlight the max number of outstanding requests.
         * @return a reference to this object.
         */
        public ConcreteBuilder withMaxInFlight(int maxInFlight)
        {
            this.maxInFlight = maxInFlight;
            return self();
        }

        /**
         * A {@link RiakOption} to use with each operation.
         *
         * @param option an option
         * @param value  the option's associated value
         * @param <U>    the type of the option's value
         * @return a reference to this object.
         */
        public <U> ConcreteBuilder withOption(RiakOption<U> option, U value)
        {
            this.options.put(option, value);
            return self();
        }

        protected abstract ConcreteBuilder self();

        public abstract BuiltType build();

    }

    public static class Response<BaseResponseType> implements Iterable<RiakFuture<BaseResponseType, Location>>
    {
        private final List<RiakFuture<BaseResponseType, Location>> responses;

        Response(List<RiakFuture<BaseResponseType, Location>> responses)
        {
            this.responses = responses;
        }

        @Override
        public Iterator<RiakFuture<BaseResponseType, Location>> iterator()
        {
            return unmodifiableList(responses).iterator();
        }

        public List<RiakFuture<BaseResponseType, Location>> getResponses()
        {
            return responses;
        }

    }

    class Submitter implements Runnable, RiakFutureListener<BaseResponseType, Location>
    {
        private final List<BaseCommand> commands;
        private final Semaphore inFlight;
        private final AtomicInteger received = new AtomicInteger();
        private final RiakCluster cluster;
        private final MultiFuture multiFuture;

        Submitter(List<BaseCommand> commands,
                         int maxInFlight,
                         RiakCluster cluster,
                         MultiFuture multiFuture)
        {
            this.commands = commands;
            this.cluster = cluster;
            this.multiFuture = multiFuture;
            inFlight = new Semaphore(maxInFlight);
        }

        @Override
        public void run()
        {
            for (BaseCommand command : commands)
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

                RiakFuture<BaseResponseType, Location> future = executeBaseCommandAsync(command, cluster);
                future.addListener(this);
            }
        }

        @Override
        public void handle(RiakFuture<BaseResponseType, Location> f)
        {
            multiFuture.addFetchFuture(f);
            inFlight.release();
            int completed = received.incrementAndGet();
            if (completed == commands.size())
            {
                multiFuture.setCompleted();
            }
        }

    }

    class MultiFuture extends ListenableFuture<ResponseType,List<Location>>
    {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final List<Location> locations;
        private final List<RiakFuture<BaseResponseType, Location>> futures;
        private volatile Throwable exception;

        MultiFuture(List<Location> locations)
        {
            this.locations = locations;
            futures = Collections.synchronizedList(new LinkedList<RiakFuture<BaseResponseType, Location>>());
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public ResponseType get() throws InterruptedException
        {
            latch.await();
            return createResponseType(futures);
        }

        @Override
        public ResponseType get(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
            if (isDone())
            {
                return createResponseType(futures);
            }
            else
            {
                return null;
            }
        }

        @Override
        public ResponseType getNow()
        {
            if (isDone())
            {
                return createResponseType(futures);
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

        private void addFetchFuture(RiakFuture<BaseResponseType, Location> future)
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
