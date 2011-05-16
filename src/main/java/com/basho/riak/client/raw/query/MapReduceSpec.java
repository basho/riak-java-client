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
package com.basho.riak.client.raw.query;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.query.MapReduce;
import com.basho.riak.client.raw.RawClient;

/**
 * An immutable representation of a Map Reduce Query, run it via
 * {@link IRiakClient#mapReduce(MapReduceSpec)}, used internally.
 * 
 * <p>
 * This is a just a tag class wrapped around a string. Use a {@link MapReduce}
 * operation to generate and execute a Map/Reduce job. Obtain a
 * {@link MapReduce} from either {@link IRiakClient#mapReduce()} or
 * {@link IRiakClient#mapReduce(String)}.
 * 
 * </p>
 * 
 * @author russell
 * @see RawClient#mapReduce(MapReduceSpec)
 * @see MapReduce
 * @see IRiakClient#mapReduce()
 * @see IRiakClient#mapReduce(String)
 */
public class MapReduceSpec {

    private final String mapReduceSpecJSON;

    /**
     * Create a MapReduceSpec by providing a map reduce job JSON string.
     * @param mapReduceSpecJSON a String of JSON describing the m/r job.
     */
    public MapReduceSpec(String mapReduceSpecJSON) {
        this.mapReduceSpecJSON = mapReduceSpecJSON;
    }

    /**
     * Get the JSON of the m/r job spec
     * @return a JSON String of m/r job spec.
     */
    public String getJSON() {
        return mapReduceSpecJSON;
    }
}
