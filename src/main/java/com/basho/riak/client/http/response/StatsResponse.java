/*
 * Copyright 2012 roach.
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
package com.basho.riak.client.http.response;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author roach
 */
public class StatsResponse extends HttpResponseDecorator implements HttpResponse
{
    private final JSONObject stats;
    
    /**
     * Create <code>StatsResponse</code> object that parses the resppnse body json
     * and makes available all of the stats from the Riak <code>/stats</code> operation
     * 
     * @param r {@link HttpResponse} from Riak <code>/stats</code>
     * @throws JSONException if the body of the response does not contain valid JSON 
     */
    public StatsResponse(final HttpResponse r) throws JSONException {
        super(r);
      
        if (r != null && r.isSuccess())
        {
            stats = new JSONObject(r.getBodyAsString());
        }
        else 
        {
            if (r != null)
                throw new RiakResponseRuntimeException(r, r.getBodyAsString());
            else
                throw new RiakResponseRuntimeException(r, "HttpResponse is null");
        }
            
    }
    
    /**
     * Get the {@link JSONObject} containing the stats returned by the Riak node
     * @return a {@link JSONObject} which can be parsed to retrieve the stats
     */
    public JSONObject getStats()
    {
        return stats;
    }
    
}
