package com.basho.riak.client.jiak;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.WalkResponse;
import com.basho.riak.client.util.Constants;

public class JiakWalkResponse implements HttpResponse, WalkResponse {

    private HttpResponse impl;
    
    public List<? extends List<JiakObject>> getSteps() { return steps; }
    private List<? extends List<JiakObject>> steps;

    public JiakWalkResponse(HttpResponse r) throws JSONException {
        this.impl = r;
        this.steps = parseSteps(r.getBody());
    }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }

    private static List<? extends List<JiakObject>> parseSteps(String body) throws JSONException {
        List<List<JiakObject>> steps = new ArrayList<List<JiakObject>>();
        final JSONArray jsonResults = new JSONObject(body).getJSONArray(Constants.JIAK_WALK_RESULTS);
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
