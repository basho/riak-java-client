package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import java.util.Objects;

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
public final class StoreIndex extends AsIsRiakCommand<Void, YokozunaIndex>
{
    private final Builder cmdBuilder;

    StoreIndex(Builder builder)
    {
        this.cmdBuilder = builder;
    }

    @Override
    protected YzPutIndexOperation buildCoreOperation()
    {
        final YzPutIndexOperation.Builder opBuilder = new YzPutIndexOperation.Builder(cmdBuilder.index);

        if (cmdBuilder.timeout != null)
        {
            opBuilder.withTimeout(cmdBuilder.timeout);
        }

        return opBuilder.build();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StoreIndex)) {
            return false;
        }
        Builder otherStoreIndex = ((StoreIndex) other).cmdBuilder;
        return Objects.equals(cmdBuilder.index, otherStoreIndex.index)
                && Objects.equals(cmdBuilder.timeout, otherStoreIndex.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmdBuilder.index, cmdBuilder.timeout);
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
