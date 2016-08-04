package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Performs a 2i query across the special $bucket index, for a known bucket, and returns the keys in that bucket.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * A BucketIndexQuery is used when you want to fetch all the keys for a bucket. Only a namespace is needed.
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * BucketIndexQuery q = new BucketIndexQuery.Builder(ns).build();
 * RawIndexquery.Response resp = client.execute(q);}</pre>
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.7
 */
public class BucketIndexQuery extends RawIndexQuery
{
    private BucketIndexQuery(Init<BinaryValue, Builder> builder)
    {
        super(builder);
    }

    public static class Builder extends SecondaryIndexQuery.Init<BinaryValue, Builder>
    {
        public Builder(Namespace namespace)
        {
            super(namespace, "$bucket", namespace.getBucketName());
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public BucketIndexQuery build()
        {
            return new BucketIndexQuery(this);
        }
    }
}
