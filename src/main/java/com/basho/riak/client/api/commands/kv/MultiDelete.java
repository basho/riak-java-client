package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.commands.kv.DeleteValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;

import java.util.List;

/**
 * Command used to delete multiple values from Riak.
 * <p>
 * Riak itself does not support pipelining of requests. MultiDelete addresses this issue by using a thread to
 * parallelize and manage a set of async delete operations for a given set of keys.
 * </p>
 * <p>
 * The result of executing this command is a {@code List} of {@link RiakFuture} objects, each one representing a single
 * delete operation. The returned {@code RiakFuture} that contains that list completes
 * when all the DeleteValue operations contained have finished.
 * <p/>
 * <pre class="prettyprint">
 * {@code
 * MultiDelete multiDelete = ...;
 * MultiDelete.Response response = client.execute(multiDelete);}
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
 * <p>
 * Be aware that for responsiveness to the caller thread, the requests are run on a Daemon worker thread.
 * Shutting down the client JVM before the future is complete will result in unexpected behavior,
 * and only a subset objects may have been deleted.
 * </p>
 * @author Gerard Stannard <gerards at tacklocal dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.7
 */
public final class MultiDelete extends MultiCommand<DeleteValue, DeleteValue.Builder, MultiDelete.Response, Void>
{
    private MultiDelete(Builder builder)
    {
        super(builder);
    }

    @Override
    protected Response createResponseType(List<RiakFuture<Void, Location>> riakFutures)
    {
        return new Response(riakFutures);
    }

    @Override
    protected DeleteValue.Builder createBaseBuilderType(Location location)
    {
        return new DeleteValue.Builder(location);
    }

    @Override
    protected RiakFuture<Void, Location> executeBaseCommandAsync(DeleteValue command, RiakCluster cluster)
    {
        return command.executeAsync(cluster);
    }

    /**
     * Used to construct a MultiDelete command.
     */
    public static class Builder extends MultiCommand.Builder<MultiDelete, Builder>
    {
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
         * Build a {@link MultiDelete} operation from this builder
         *
         * @return an initialized {@link MultiDelete} operation
         */
        @Override
        public MultiDelete build()
        {
            return new MultiDelete(this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }

    public static class Response extends MultiCommand.Response<Void>
    {
        Response(List<RiakFuture<Void, Location>> responses)
        {
            super(responses);
        }
    }
}
