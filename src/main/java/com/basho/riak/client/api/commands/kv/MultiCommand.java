package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.ListenableFuture;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.query.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;

/**
 * Runs multiple individual commands together on a worker daemon thread.
 * @param <BaseCommand> The type of the individual command you are trying to repeat
 * @param <BaseBuilder> The type of the builder for an individual {@link BaseCommand}
 * @param <ResponseType> The return type of the grouped "multi" command
 * @param <BaseResponseType> The return type of an individual {@link BaseCommand}
 */
/*

 */
abstract class MultiCommand<BaseCommand extends RiakCommand<BaseResponseType, Location>,
                                   BaseBuilder extends KvBuilderBase<BaseCommand>,
                                   ResponseType extends Iterable<RiakFuture<BaseResponseType, Location>>,
                                   BaseResponseType>
        extends RiakCommand<ResponseType, List<Location>>
{
    private static final int DEFAULT_MAX_IN_FLIGHT = 10;

    private final ArrayList<Location> locations;
    protected final Map<RiakOption<?>, Object> options = new HashMap<>();
    private final int maxInFlight;

    @SuppressWarnings("unchecked")
    MultiCommand(Builder builder)
    {
        this.locations = builder.locations;
        this.options.putAll(builder.options);
        this.maxInFlight = builder.maxInFlight;
    }

    @Override
    protected RiakFuture<ResponseType, List<Location>> executeAsync(final RiakCluster cluster)
    {
        List<BaseCommand> operations = buildOperations();
        MultiFuture future = new MultiFuture(locations);

        Submitter submitter = new Submitter(operations, maxInFlight, cluster, future);

        Thread worker = new Thread(submitter);
        worker.setDaemon(true);
        worker.start();

        return future;
    }

    @SuppressWarnings("unchecked")
    private List<BaseCommand> buildOperations()
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
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        MultiCommand<?, ?, ?, ?> that = (MultiCommand<?, ?, ?, ?>) o;

        if (maxInFlight != that.maxInFlight)
        {
            return false;
        }
        if (!locations.equals(that.locations))
        {
            return false;
        }
        return options.equals(that.options);
    }

    @Override
    public int hashCode()
    {
        int result = locations.hashCode();
        result = 31 * result + options.hashCode();
        result = 31 * result + maxInFlight;
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("%s {locations: %s, options: %s, maxInFlight: %s}",
                             this.getClass().getSimpleName(), locations, options, maxInFlight);
    }

    protected abstract ResponseType createResponseType(List<RiakFuture<BaseResponseType, Location>> futures);
    protected abstract BaseBuilder createBaseBuilderType(Location location);
    protected abstract RiakFuture<BaseResponseType, Location> executeBaseCommandAsync(BaseCommand command,
                                                                                      RiakCluster cluster);

    protected static abstract class Builder<BuiltType, ConcreteBuilder extends Builder<BuiltType, ConcreteBuilder>>
    {
        private ArrayList<Location> locations = new ArrayList<>();
        private Map<RiakOption<?>, Object> options = new HashMap<>();
        private int maxInFlight = DEFAULT_MAX_IN_FLIGHT;

        /**
         * Add a location to the list of locations to interact with as part of
         * this operation.
         *
         * @param location the location to add.
         * @return this
         */
        public ConcreteBuilder addLocation(Location location)
        {
            locations.add(location);
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
            locations.addAll(Arrays.asList(location));
            return self();
        }

        /**
         * Add a set of locations to the list of Locations to interact with as part of
         * this operation.
         *
         * @param location an Iterable set of Locations.
         * @return a reference to this object
         */
        public ConcreteBuilder addLocations(Iterable<Location> location)
        {
            for (Location loc : location)
            {
                locations.add(loc);
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
        private final Logger logger = LoggerFactory.getLogger(this.getClass());
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
            logger.debug("Running daemon worker thread.");
            for (BaseCommand command : commands)
            {
                try
                {
                    inFlight.acquire();
                }
                catch (InterruptedException ex)
                {
                    logger.error("Daemon worker thread interrupted.");
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
            logger.debug("Received MultiCommand individual result.");
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

            // If we have no locations, then we have no work to do.
            if (this.locations.isEmpty())
            {
                setCompleted();
            }
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
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException
        {
            return latch.await(timeout, unit);
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
