package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;


/**
 * Command used to delete a search index in Riak.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteIndex extends RiakCommand<Void, String>
{
    private final String index;

    DeleteIndex(Builder builder)
    {
        this.index = builder.index;
    }

    @Override
    protected final RiakFuture<Void, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, String> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Void, String, Void, String> future =
            new CoreFutureAdapter<Void, String, Void, String>(coreFuture)
    {
        
        @Override
        protected Void convertResponse(Void coreResponse)
        {
            return coreResponse;
        }

        @Override
        protected String convertQueryInfo(String coreQueryInfo)
        {
            return coreQueryInfo;
        }
            };
        coreFuture.addListener(future);
        return future;
    }
    
    private YzDeleteIndexOperation buildCoreOperation()
    {
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
