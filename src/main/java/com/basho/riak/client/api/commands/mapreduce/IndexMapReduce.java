package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Command used to performs a map reduce operation using a secondary index (2i) as input.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class IndexMapReduce extends MapReduce
{
    protected IndexMapReduce(IndexInput input, Builder builder)
    {
        super(input, builder);
    }

    /**
     * Builder for a IndexMapReduce command.
     */
    public static class Builder extends MapReduce.Builder<Builder>
    {
        private Namespace namespace;
        private String index;
        private IndexInput.IndexCriteria criteria;

        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Add the namespace that the index resides in to the builder.
         * @param namespace the namespace that the index resides in
         * @return a reference to this object.
         */
        public Builder withNamespace(Namespace namespace)
        {
            this.namespace = namespace;
            return this;
        }

        /**
         * Add the index to query to the builder.
         * @param index the secondary index to query
         * @return a reference to this object.
         */
        public Builder withIndex(String index)
        {
            this.index = index;
            return this;
        }

        /**
         * Set the query to a Range query on the builder.
         * Overrides any previous index queries set on this builder instance.
         * @param start The inclusive lower bound of the index query
         * @param end The inclusive upper bound of the index query
         * @return a reference to this object.
         */
        public Builder withRange(final long start, final long end)
        {
            this.criteria = new IndexInput.RangeCriteria<>(start, end);
            return this;
        }

        /**
         * Set the query to a Range query on the builder.
         * Overrides any previous index queries set on this builder instance.
         * @param start The inclusive lower bound of the index query
         * @param end The inclusive upper bound of the index query
         * @return a reference to this object.
         */
        public Builder withRange(final BinaryValue start, final BinaryValue end)
        {
            this.criteria = new IndexInput.RangeCriteria<>(start, end);
            return this;
        }

        /**
         * Set the query to a Match query on the builder.
         * Overrides any previous index queries set on this builder instance.
         * @param value the index value to match
         * @return a reference to this object.
         */
        public Builder withMatchValue(final long value)
        {
            this.criteria = new IndexInput.MatchCriteria<>(value);
            return this;
        }

        /**
         * Set the query to a Match query on the builder.
         * Overrides any previous index queries set on this builder instance.
         * @param value the index value to match
         * @return a reference to this object.
         */
        public Builder withMatchValue(final BinaryValue value)
        {
            this.criteria = new IndexInput.MatchCriteria<>(value);
            return this;
        }

        /**
         * Construct a new IndexMapReduce operation.
         * @return the new IndexMapReduce operation.
         */
        public IndexMapReduce build()
        {
            if (namespace == null)
            {
                throw new IllegalStateException("A namespace must be specified");
            }

            if (index == null)
            {
                throw new IllegalStateException("An index must be specified");
            }

            if (criteria == null)
            {
                throw new IllegalStateException("An index search criteria must be specified");
            }

            return new IndexMapReduce(new IndexInput(namespace, index, criteria), this);
        }
    }
}
