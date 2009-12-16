package com.basho.riak.client.response;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

public interface StreamHandler {
    
    /**
     * Process the HTTP response whose value is given as a stream.
     * 
     * @param bucket The object's bucket
     * @param bucket The object's key
     * @param status The HTTP status code returned for the request
     * @param headers The HTTP headers returned in the response
     * @param in InputStream of the object's value (body)
     * @param httpMethod The original {@link HttpMethod} used to make the request. 
     *        Its connection is still open and will be closed by the caller on return. 
     * @return true if the object was processed; false otherwise
     */
    public boolean process(String bucket, String key, int status, Map<String, String> headers, InputStream in, HttpMethod httpMethod);
}
