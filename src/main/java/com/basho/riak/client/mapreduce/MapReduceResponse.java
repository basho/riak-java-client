package com.basho.riak.client.mapreduce;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;

import com.basho.riak.client.response.HttpResponse;

/**
 * Wrapper to handle map/reduce job responses
 */
public class MapReduceResponse {

    private int status = -1;
    private String body = null;
    private HttpMethod httpMethod = null;

    public MapReduceResponse(HttpResponse r) {
        status = r.getStatusCode();
        body = r.getBody();
        httpMethod = r.getHttpMethod();
    }

    /**
     * The HTTP status code returned by the Riak server
     */
    public int getStatusCode() {
        return status;
    }

    /**
     * The raw String-ified body of the server response
     */
    public String getBody() {
        return body;
    }

    /**
     * Try to parse the response body and return it as JSON
     * 
     * @throws JSONException
     */
    public JSONArray getParsedBody() throws JSONException {
        return new JSONArray(getBody());
    }

    /**
     * The actual {@link HttpMethod} used to make the HTTP request.
     */
    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    /**
     * Does the status code indicate success
     */
    public boolean isSuccess() {
        return status >= 200 && status < 300;
    }

    /**
     * Does the status code indicate failure
     */
    public boolean isError() {
        return !this.isSuccess();
    }
}
