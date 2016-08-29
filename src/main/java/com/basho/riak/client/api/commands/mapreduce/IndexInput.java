package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Namespace;

/**
 * MapReduce Input that uses the results of a Secondary Index(2i) query as the input
 * for a MapReduce job.
 */
public class IndexInput implements MapReduceInput
{
    private final Namespace namespace;
    private final String index;
    private final IndexCriteria criteria;

    /**
     * Construct an IndexInput using the provided namespace, index name, and criteria.
     * @param namespace the namespace that the index resides in
     * @param index the secondary index to query
     * @param criteria the secondary index criteria to search for
     */
    public IndexInput(Namespace namespace, String index, IndexCriteria criteria)
    {
        this.namespace = namespace;
        this.index = index;
        this.criteria = criteria;
    }

    /**
     * Gets the namespace of the 2i query.
     * @return the namespace of the 2i query
     */
    public Namespace getNamespace()
    {
        return namespace;
    }

    /**
     * Gets the index name of the 2i query.
     * @return the index name of the 2i query
     */
    public String getIndex()
    {
        return index;
    }

    /**
     * Gets the criteria for the 2i query.
     * @return the criteria for the 2i query
     */
    public IndexCriteria getCriteria()
    {
        return criteria;
    }

    /**
     * Interface for MapReduce 2i query criteria.
     */
    static interface IndexCriteria
    {
    }

    /**
     * A range query index criteria.
     * @param <V> Either a {@link String}, or an {@link Integer}.
     */
    static class RangeCriteria<V> implements IndexCriteria
    {
        private final V begin;
        private final V end;

        /**
         * Create a new RangeCriteria with the supplied begin and end bounds.
         * @param begin The inclusive lower bound of the index query
         * @param end The inclusive upper bound of the index query
         */
        public RangeCriteria(V begin, V end)
        {
            this.begin = begin;
            this.end = end;
        }

        /**
         * Gets the begin bound value
         * @return the begin (lower) bound
         */
        public V getBegin()
        {
            return begin;
        }

        /**
         * Gets the end bound value
         * @return the end (upper) bound
         */
        public V getEnd()
        {
            return end;
        }

    }

    /**
     * A match query index criteria.
     * @param <V> Either a {@link String}, or an {@link Integer}.
     */
    static class MatchCriteria<V> implements IndexCriteria
    {
        private final V value;

        /**
         * Create a new MatchCriteria using the supplied match value.
         * @param value the index value to match
         */
        public MatchCriteria(V value)
        {
            this.value = value;
        }

        /**
         * Gets the match value
         * @return the match value
         */
        public V getValue()
        {
            return value;
        }

    }
}
