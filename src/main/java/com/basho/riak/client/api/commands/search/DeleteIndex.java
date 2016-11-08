package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;

/**
 * Command used to delete a search index in Riak.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteIndex extends AsIsRiakCommand<Void, String>
{
    private final String index;

    DeleteIndex(Builder builder)
    {
        this.index = builder.index;
    }

    @Override
    protected YzDeleteIndexOperation buildCoreOperation() {
        return new YzDeleteIndexOperation.Builder(index).build();
    }

    /**
     * Builder for a DeleteIndex command.
     */
    public static class Builder
    {
        private final String index;

        /**
         * Construct a Builder for a DeleteIndex command.
         *
         * @param index The name of the search index to delete from Riak.
         */
        public Builder(String index)
        {
            this.index = index;
        }

        /**
         * Construct the DeleteIndex command.
         * @return the new DeleteIndex command.
         */
        public DeleteIndex build()
        {
            return new DeleteIndex(this);
        }
    }
}
