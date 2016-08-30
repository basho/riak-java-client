package com.basho.riak.client.api.commands.mapreduce;

/**
 * Command used to perform a map reduce operation with a search query as input.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class SearchMapReduce extends MapReduce
{
    protected SearchMapReduce(SearchInput input, Builder builder)
    {
        super(input, builder);
    }

    /**
     * Builder for a SearchMapReduce command.
     */
    public static class Builder extends MapReduce.Builder<Builder>
    {
        private String index;
        private String query;

        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Set the index to search.
         * @param index The index to run the search on
         * @return a reference to this object.
         */
        public Builder withIndex(String index)
        {
            this.index = index;
            return this;
        }

        /**
         * Set the query to run.
         * @param query The query to run
         * @return a reference to this object.
         */
        public Builder withQuery(String query)
        {
            this.query = query;
            return this;
        }

        /**
         * Construct a new SearchMapReduce operation.
         * @return the new SearchMapReduce operation.
         */
        public SearchMapReduce build()
        {
            if (index == null)
            {
                throw new IllegalStateException("An index must be specified");
            }

            if (query == null)
            {
                throw new IllegalStateException("A query must be specified");
            }

            return new SearchMapReduce(new SearchInput(index, query), this);
        }
    }
}
