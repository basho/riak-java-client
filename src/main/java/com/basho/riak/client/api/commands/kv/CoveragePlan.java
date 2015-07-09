package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.CoveragePlanOperation;
import com.basho.riak.client.core.query.Namespace;

public class CoveragePlan  extends RiakCommand<CoveragePlan.Response, Namespace> {
    private final CoveragePlanOperation operation;

    private CoveragePlan(Builder builder)
    {
        this.operation = builder.buildOperation();
    }

    @Override
    protected RiakFuture<Response, Namespace> executeAsync(RiakCluster cluster) {
        final RiakFuture<CoveragePlanOperation.Response, Namespace> coreFuture = cluster.execute(operation);

        CoreFutureAdapter<CoveragePlan.Response, Namespace, CoveragePlanOperation.Response, Namespace> future =
            new CoreFutureAdapter<CoveragePlan.Response, Namespace, CoveragePlanOperation.Response, Namespace>(coreFuture)
            {
                @Override
                protected Response convertResponse(CoveragePlanOperation.Response coreResponse)
                {
                    return new Response(coreResponse);
                }

                @Override
                protected Namespace convertQueryInfo(Namespace coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    /**
     * Used to construct a CoveragePlan command.
     */
    public static class Builder extends CoveragePlanOperation.AbstractBuilder<CoveragePlan> {
        public Builder(Namespace ns) {
            super(ns);
        }

        @Override
        public CoveragePlan build()
        {
            return new CoveragePlan(this);
        }

        public static Builder create(Namespace ns){
            return new Builder(ns);
        }
    }

    public static class Response extends CoveragePlanOperation.Response
    {
        private Response(CoveragePlanOperation.Response coreResponse) {
            super(coreResponse);
        }
    }
}
