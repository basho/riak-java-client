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
import com.basho.riak.client.RiakException;
import com.basho.riak.client.raw.RawClient;

/**
 * Checked exception thrown when an Map Reduce job takes longer than the
 * specified timeout.
 * 
 * @author russell
 * 
 * @see IRiakClient#mapReduce()
 * @see RawClient#mapReduce(MapReduceSpec)
 */
public class MapReduceTimeoutException extends RiakException {

    private static final long serialVersionUID = -1293682325413369755L;

    public MapReduceTimeoutException() {
        super();
    }

}
