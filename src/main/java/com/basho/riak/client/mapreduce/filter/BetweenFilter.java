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
package com.basho.riak.client.mapreduce.filter;

import org.json.JSONException;
import org.json.JSONArray;

public class BetweenFilter implements MapReduceFilter {
    private MapReduceFilter.Types type = MapReduceFilter.Types.FILTER;
    JSONArray args = new JSONArray();
    
    public BetweenFilter(String from, String to) {
        args.put("between");
        args.put(from);
        args.put(to);
    }
    
    public BetweenFilter(int from, int to) {
        args.put("between");
        args.put(from);
        args.put(to);
    }

    public BetweenFilter(long from, long to) {
        args.put("between");
        args.put(from);
        args.put(to);
    }

    public BetweenFilter(double from, double to) throws JSONException {
        args.put("between");
        args.put(from);
        args.put(to);
    }
    
    public JSONArray toJson() {
        return args;
    }
}
