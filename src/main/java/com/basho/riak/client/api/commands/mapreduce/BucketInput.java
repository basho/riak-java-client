package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.api.commands.mapreduce.filters.KeyFilter;

import java.util.Collection;

/**
 * MapReduce Input that provides a whole bucket's contents as input for the MapReduce job.
 */
public class BucketInput implements MapReduceInput
{
    private final Namespace namespace;
    private final Collection<KeyFilter> filters;

    /**
     * Creates a BucketInput from the provided namespace, and applicable keyfilters.
     * @param namespace the namespace to keylist over
     * @param filters a collection of {@link KeyFilter}s to filter the namespace's keys by
     */
    public BucketInput(Namespace namespace, Collection<KeyFilter> filters)
    {
        this.namespace = namespace;
        this.filters = filters;
    }

    /**
     * Gets the namespace of the input.
     * @return the namespace of the input.
     */
    public Namespace getNamespace()
    {
        return namespace;
    }

    /**
     * Indicates whether the input has any keyfilters to apply.
     * @return whether the input includes any keyfilters
     */
    public boolean hasFilters()
    {
        return filters != null && !filters.isEmpty();
    }

    /**
     * Gets the keyfilter collection of the input.
     * @return the keyfilter collection of the input.
     */
    public Collection<KeyFilter> getFilters()
    {
        return filters;
    }

}
