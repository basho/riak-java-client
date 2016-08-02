package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ResetBucketPropsOperation;
import com.basho.riak.client.core.query.Namespace;

/**
 * Command used to reset the properties of a bucket in Riak.
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * ResetBucketProperties rbp =
 *  new ResetBucketProperties.Builder(ns)
 *      .build();
 * client.execute(rbp);}</pre>
 * </p>
 * @author Chris Mancini <cmancini at basho dot com>
 * @since 2.0
 */
public class ResetBucketProperties extends RiakCommand<Void, Namespace>
{

    private final Namespace namespace;

    public ResetBucketProperties(Builder builder)
    {
        this.namespace = builder.namespace;
    }

    @Override
    protected final RiakFuture<Void, Namespace> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, Namespace> coreFuture =
                cluster.execute(buildCoreOperation());

        CoreFutureAdapter<Void, Namespace, Void, Namespace> future =
                new CoreFutureAdapter<Void, Namespace, Void, Namespace>(coreFuture)
                {
                    @Override
                    protected Void convertResponse(Void coreResponse)
                    {
                        return coreResponse;
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

    private ResetBucketPropsOperation buildCoreOperation()
    {
        ResetBucketPropsOperation.Builder builder =
                new ResetBucketPropsOperation.Builder(namespace);

        return builder.build();
    }

    public static class Builder
    {

        private final Namespace namespace;

        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            this.namespace = namespace;
        }

        public ResetBucketProperties build()
        {
            return new ResetBucketProperties(this);
        }

    }
}
