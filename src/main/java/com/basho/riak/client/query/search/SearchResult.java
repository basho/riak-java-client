/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.query.search;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the result set of a Riak Search or Yokuzuna query.
 * <p>
 * Each result is returned as a Map with the field names as the keys. 
 * </p>
 * <p>
 * Due to the nature of both Riak Search and Yokozuna, all Strings are 
 * UTF-8 encoded.
 * </p>
 * @riak.threadsafety The List and the Maps in the list are immutable making this
 * container threadsafe.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class SearchResult implements Iterable
{
    private final List<Map<String, String>> results;
    private final float maxScore;
    private final int numResults; 
    
    public SearchResult(List<Map<String,String>> results, float maxScore, int numResults)
    {
        this.results = results;
        this.maxScore = maxScore;
        this.numResults = numResults;
    }

    @Override
    public Iterator<Map<String,String>> iterator()
    {
        return results.iterator();
    }
    
    /**
     * Returns the max score from the search query.
     * @return the max score.
     */
    public float getMaxScore()
    {
        return maxScore;
    }
    
    /**
     * Returns the number of results from the search query.
     * @return the number of results.
     */
    public int numResults()
    {
        return numResults;
    }
    
    /**
     * Returns the entire list of results from the search query.
     * @return a list containing all the result sets. 
     */
    public List<Map<String,String>> getAllResults()
    {
        return results;
    }
    
}
