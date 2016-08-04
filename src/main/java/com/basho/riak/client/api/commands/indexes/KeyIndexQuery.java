package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.DefaultCharset;

/**
 * Performs a 2i query across the special $key index, for a known bucket & range of keys,
 * and returns the keys within that range in that bucket.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * A KeyIndexQuery is used when you want to fetch a range of keys for a bucket. A namespace and a key range is needed.
 * </p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * KeyIndexQuery q = new KeyIndexQuery.Builder(ns, "foo10", "foo19").build();
 * RawIndexquery.Response resp = client.execute(q);}</pre>
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.7
 */
public class KeyIndexQuery extends RawIndexQuery
{
    private KeyIndexQuery(Init<BinaryValue, Builder> builder)
    {
        super(builder);
    }

    public static class Builder extends Init<BinaryValue, Builder>
    {
        public Builder(Namespace namespace, String start, String end)
        {
            this(namespace,
                 BinaryValue.unsafeCreate(start.getBytes(DefaultCharset.get())),
                 BinaryValue.unsafeCreate(end.getBytes(DefaultCharset.get())));
        }

        public Builder(Namespace namespace, BinaryValue start, BinaryValue end)
        {
            super(namespace, "$key", start, end);
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public KeyIndexQuery build()
        {
            return new KeyIndexQuery(this);
        }
    }
}
