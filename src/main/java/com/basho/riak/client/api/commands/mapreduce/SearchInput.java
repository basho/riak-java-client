package com.basho.riak.client.api.commands.mapreduce;

/**
 * MapReduce Input that provides the results of a Search query
 * as the input for a MapReduce job.
 */
public class SearchInput implements MapReduceInput
{
    private final String index;
    private final String search;

    /**
     * Construct a SearchInput using the provided index name and search query.
     * @param index The name of the index to query
     * @param search The search query to run on the index
     */
    public SearchInput(String index, String search)
    {
        this.index = index;
        this.search = search;
    }

    /**
     * Gets the index supplied for the search.
     * @return the name of the index to search
     */
    public String getIndex()
    {
        return index;
    }

    /**
     * Gets the search query supplied for the search.
     * @return the query to run on the index to search
     */
    public String getSearch()
    {
        return search;
    }
}
