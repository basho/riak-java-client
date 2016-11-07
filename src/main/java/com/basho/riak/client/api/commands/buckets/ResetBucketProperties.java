package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.GenericRiakCommand;
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
public class ResetBucketProperties extends GenericRiakCommand<Void, Namespace, Void, Namespace>
{
    private final Namespace namespace;

    public ResetBucketProperties(Builder builder)
    {
        this.namespace = builder.namespace;
    }

    @Override
    protected ResetBucketPropsOperation buildCoreOperation() {
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
