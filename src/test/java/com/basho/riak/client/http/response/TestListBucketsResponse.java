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
package com.basho.riak.client.http.response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.json.JSONException;
import org.junit.Test;

/**
 * @author russell
 * 
 */
public class TestListBucketsResponse {

    private static final String BUCKET_1 = "my_test_bucket_one";
    private static final String BUCKET_2 = "my_test_bucket_two";
    private static final String BUCKET_BODY = "{\"buckets\": [\"" + BUCKET_1 + "\", \"" + BUCKET_2 + "\" ]}";

    private static final String EMPTY_BODY = "{\"buckets\": []}";

    /**
     * Test method for
     * {@link com.basho.riak.client.http.response.ListBucketsResponse#getBuckets()}
     * .
     * 
     * @throws IOException
     * @throws JSONException
     */
    @Test public void getBuckets_nullRespZeroBuckets() throws JSONException, IOException {
        final ListBucketsResponse lbr = new ListBucketsResponse(null);
        assertTrue(lbr.getBuckets().isEmpty());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.http.response.ListBucketsResponse#getBuckets()}
     * .
     * 
     * @throws IOException
     * @throws JSONException
     */
    @Test public void getBuckets_empty() throws JSONException, IOException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBodyAsString()).thenReturn(EMPTY_BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        final ListBucketsResponse lbr = new ListBucketsResponse(mockHttpResponse);
        assertTrue(lbr.getBuckets().isEmpty());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.http.response.ListBucketsResponse#getBuckets()}
     * .
     * 
     * @throws IOException
     * @throws JSONException
     */
    @Test public void getBuckets() throws JSONException, IOException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBodyAsString()).thenReturn(BUCKET_BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        final ListBucketsResponse lbr = new ListBucketsResponse(mockHttpResponse);

        assertTrue("Expected BUCKET_1 to be in response", lbr.getBuckets().contains(BUCKET_1));
        assertTrue("Expected BUCKET_2 in response", lbr.getBuckets().contains(BUCKET_2));
        assertEquals("Expected 2 buckets in response", 2, lbr.getBuckets().size());
    }
}
