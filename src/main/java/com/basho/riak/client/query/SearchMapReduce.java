/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.query;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.basho.riak.client.raw.RawClient;

/**
 * A {@link MapReduce} operation that uses a Riak Search query as input.
 * 
 * See <a href="http://wiki.basho.com/Riak-Search---Querying.html#Querying-Integrated-with-Map-Reduce">
 * Riak Search
 * </a> on the basho wiki for more information.
 * 
 * @author russell
 * 
 */
public class SearchMapReduce extends MapReduce {

    private final String bucket;
    private final String query;

    /**
     * Create a map/reduce using a riak search query as input
     * 
     * @param client
     *            the {@link RawClient} to execute the m/r job
     * @param bucket
     *            the search indexed bucket
     * @param query
     *            the search query
     */
    public SearchMapReduce(final RawClient client, String bucket, String query) {
        super(client);
        this.bucket = bucket;
        this.query = query;
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @return the query
     */
    public String getQuery() {
        return query;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.query.MapReduce#writeInput(org.codehaus.jackson
     * .JsonGenerator)
     */
    @Override protected void writeInput(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeObject(new Object() {
            @JsonProperty String module = "riak_search";
            @JsonProperty String function = "mapred_search";
            @JsonProperty String[] arg = new String[] { bucket, query };
        });
    }

}
