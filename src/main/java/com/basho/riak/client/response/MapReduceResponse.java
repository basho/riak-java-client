package com.basho.riak.client.response;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * Response from a map-reduce query (POST to /mapred). Decorates an HttpResponse
 * and parses returned JSON array returned from Riak.
 */
public class MapReduceResponse extends HttpResponseDecorator implements HttpResponse {

    JSONArray result = null;

    /**
     * On a 2xx response, parses the response into a {@link JSONArray}
     * 
     * @param r
     *            The HTTP response query POST'd to the map-reduce resource
     * @throws JSONException
     *             Response is a 2xx but doesn't contain a valid JSON array
     */
    public MapReduceResponse(HttpResponse r) throws JSONException {
        super(r);

        if (r != null && r.isSuccess() && (r.getBody() != null)) {
            result = new JSONArray(r.getBody());
        }
    }

    /**
     * The result of the map-reduce query as a JSON array
     */
    public JSONArray getResults() {
        return result;
    }
}
