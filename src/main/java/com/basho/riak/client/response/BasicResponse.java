package com.basho.riak.client.response;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;

import com.basho.riak.client.util.ClientUtils;

public class BasicResponse implements HttpResponse {

    public BasicResponse(String bucket, String key, int status, Map<String, String> headers, String body, HttpMethod httpMethod) {
        this.bucket = bucket;
        this.key = key;
        this.statusCode = status;
        this.headers = headers;
        this.body = body;
        this.httpMethod = httpMethod;
    }

    public String getBucket() { return bucket; }
    private String bucket;

    public String getKey() { return key; }
    private String key;
    
    /** The resulting status code from the HTTP request. */ 
    public int getStatusCode() { return statusCode; }
    private int statusCode = -1; 
    
    /** The HTTP response headers. */ 
    public Map<String, String> getHttpHeaders() { return headers; }
    private Map<String, String> headers = null;
    
    public String getBody() { return body; }
    private String body = null;

    /** The actual {@link HttpMethod} used to make the HTTP request. 
     *  Most of the data here can be retrieved more simply using 
     *  methods in this class. Also, Note that the connection will already 
     *  be closed, so calling getHttpMethod().getResponseBodyAsStream() 
     *  will return null.*/
    public HttpMethod getHttpMethod() { return httpMethod; }
    private HttpMethod httpMethod = null;

    /** Did the HTTP request return a 2xx (or 404 in case of DELETE) response? */ 
    public boolean isSuccess() { 
        return (statusCode >= 200 && statusCode < 300) ||
               (statusCode == 404 && (httpMethod instanceof DeleteMethod)); 
    }

    /** Did the HTTP request return a 4xx or 5xx response? */ 
    public boolean isError() { 
        return statusCode >= 400; 
    }

    public static BasicResponse fromHttpMethod(String bucket, String key, final HttpMethod httpMethod) throws IOException {
        int status = httpMethod.getStatusCode();
        Map<String, String> headers = ClientUtils.asHeaderMap(httpMethod.getResponseHeaders());
        String body = httpMethod.getResponseBodyAsString();
        return new BasicResponse(bucket, key, status, headers, body, httpMethod);
    }
}
