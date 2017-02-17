package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.ListException;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.api.commands.mapreduce.filters.KeyFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Command used to perform a Map Reduce operation over a bucket in Riak.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class BucketMapReduce extends MapReduce
{
    protected BucketMapReduce(BucketInput input, Builder builder) throws ListException
    {
        super(input, builder);

        if (builder.allowListing == false)
        {
            throw new ListException();
        }
    }

    /**
     * Builder for a BucketMapReduce command.
     */
    public static class Builder extends MapReduce.Builder<Builder>
    {
        private Namespace namespace;
        private final List<KeyFilter> filters = new ArrayList<>();
        private boolean allowListing;

        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Allow this listing command
         * <p>
         * Bucket and key list operations are expensive and should not
         * be used in production, however using this method will allow
         * the command to be built.
         * </p>
         * @return a reference to this object.
         */
            public Builder withAllowListing()
        {
            this.allowListing = true;
            return this;
        }

        public Builder withNamespace(Namespace namespace)
        {
            this.namespace = namespace;
            return this;
        }

        public Builder withKeyFilter(KeyFilter filter)
        {
            filters.add(filter);
            return this;
        }

        public BucketMapReduce build() throws ListException
        {
            if (namespace == null)
            {
                throw new IllegalStateException("A Namespace must be specified");
            }

            return new BucketMapReduce(new BucketInput(namespace, filters), this);
        }
    }
}
