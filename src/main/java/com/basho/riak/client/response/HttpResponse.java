package com.basho.riak.client.response;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

public interface HttpResponse {

    public String getBucket();

    public String getKey();
    
    /** The resulting status code from the HTTP request. */ 
    public int getStatusCode(); 
    
    /** The HTTP response headers. */ 
    public Map<String, String> getHttpHeaders();
    
    public String getBody();

    /** The actual {@link HttpMethod} used to make the HTTP request. 
     *  Most of the data here can be retrieved more simply using 
     *  methods in this class. Also, Note that the connection will already 
     *  be closed, so calling getHttpMethod().getResponseBodyAsStream() 
     *  will return null.*/
    public HttpMethod getHttpMethod();
    
    /** Did the HTTP request return a 2xx (or 404 in case of DELETE) response? */ 
    public boolean isSuccess();

    /** Did the HTTP request return a 4xx or 5xx response? */ 
    public boolean isError();

}
