package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.core.query.search.YokozunaIndex;

/**
 * Command used to store a search index in Riak.
 * <p>
 * To store/create an index for Solr/Yokozuna in Riak, you must supply a
 * {@link com.basho.riak.client.core.query.search.YokozunaIndex} that defines the index and it's properties.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreIndex extends RiakCommand<Void, YokozunaIndex>
{
    private final Builder cmdBuilder;

    StoreIndex(Builder builder)
    {
        this.cmdBuilder = builder;
    }

    @Override
    protected RiakFuture<Void, YokozunaIndex> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, YokozunaIndex> coreFuture = cluster.execute(buildCoreOperation());

        CoreFutureAdapter<Void, YokozunaIndex, Void, YokozunaIndex> future =
                new CoreFutureAdapter<Void, YokozunaIndex, Void, YokozunaIndex>(coreFuture)
                {
                    @Override
                    protected Void convertResponse(Void coreResponse)
                    {
                        return coreResponse;
                    }

                    @Override
                    protected YokozunaIndex convertQueryInfo(YokozunaIndex coreQueryInfo)
                    {
                        return coreQueryInfo;
                    }
                };
        coreFuture.addListener(future);
        return future;
    }

    private YzPutIndexOperation buildCoreOperation()
    {
        final YzPutIndexOperation.Builder opBuilder = new YzPutIndexOperation.Builder(cmdBuilder.index);

        if (cmdBuilder.timeout != null)
        {
            opBuilder.withTimeout(cmdBuilder.timeout);
        }

        return opBuilder.build();
    }

    /**
     * Builder for a StoreIndex command.
     */
    public static class Builder
    {
        private final YokozunaIndex index;
        private Integer timeout;

        /**
         * Construct a Builder for a StoreIndex command.
         *
         * @param index The index to create or edit in Riak.
         */
        public Builder(YokozunaIndex index)
        {
            this.index = index;
        }

        /**
         * Set the Riak-side timeout value, available in <b>Riak 2.1</b> and later.
         * <p>
         * By default, riak has a 45s timeout for Yokozuna index creation.
         * Setting this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            this.timeout = timeout;
            return this;
        }

        /**
         * Construct the StoreIndex command.
         *
         * @return the new StoreIndex command.
         */
        public StoreIndex build()
        {
            return new StoreIndex(this);
        }
    }
}
