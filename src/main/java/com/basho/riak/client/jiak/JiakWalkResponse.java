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
package com.basho.riak.client.jiak;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;
import com.basho.riak.client.response.WalkResponse;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret walk responses from Jiak.
 */
public class JiakWalkResponse extends HttpResponseDecorator implements WalkResponse {

    private List<? extends List<JiakObject>> steps = new ArrayList<List<JiakObject>>();

    /**
     * On a 2xx response, parses the HTTP body into a list of steps. Each step
     * contains a list of objects returned in that step. The HTTP body is JSON
     * with a "results" field.
     */
    public JiakWalkResponse(HttpResponse r) throws JSONException {
        super(r);

        if (r != null && r.isSuccess() && (r.getBody() != null)) {
            steps = parseSteps(r.getBody());
        }
    }

    public boolean hasSteps() {
        return steps.size() > 0;
    }

    public List<? extends List<JiakObject>> getSteps() {
        return steps;
    }

    /**
     * Parse a JSON blob into a list of lists of {@link JiakObject}s
     * 
     * @param body
     *            JSON representation of walk results containing a "results"
     *            field
     * @return List of lists of {@link JiakObjects} represented by the JSON
     * @throws JSONException
     *             If the "results" field is missing or any step is not a
     *             properly formed array of Jiak objects.
     * 
     */
    private static List<? extends List<JiakObject>> parseSteps(String body) throws JSONException {
        List<List<JiakObject>> steps = new ArrayList<List<JiakObject>>();
        final JSONArray jsonResults = new JSONObject(body).getJSONArray(Constants.JIAK_FL_WALK_RESULTS);
        for (int i = 0; i < jsonResults.length(); ++i) {
            final ArrayList<JiakObject> step = new ArrayList<JiakObject>();
            final JSONArray jsonStep = jsonResults.getJSONArray(i);
            for (int j = 0; j < jsonStep.length(); ++j) {
                final JSONObject json = jsonStep.getJSONObject(j);
                step.add(new JiakObject(json));
            }
            steps.add(step);
        }
        return steps;
    }
}
