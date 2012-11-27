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
package com.basho.riak.client.raw.http;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.AllClientPNames;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.HttpParams;
import org.junit.Test;

import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author russell
 * 
 */
public class HTTPRiakClientFactoryTest {

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.HTTPRiakClientFactory#getInstance()}
     * .
     */
    @Test public void instanceIsPopulates() {
        assertNotNull(HTTPRiakClientFactory.getInstance());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.HTTPRiakClientFactory#accepts(java.lang.Class)}
     * .
     */
    @Test public void acceptsOnlyHTTPClientConfig() {
        RiakClientFactory fac = HTTPRiakClientFactory.getInstance();
        assertTrue(fac.accepts(HTTPClientConfig.class));
        assertFalse(fac.accepts(PBClientConfig.class));
        assertFalse(fac.accepts(ClusterConfig.class));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.HTTPRiakClientFactory#newClient(com.basho.riak.client.raw.config.Configuration)}
     * .
     * 
     * @throws IOException
     */
    @Test public void newClientIsConfigured() throws IOException {
        HTTPClientConfig.Builder b = new HTTPClientConfig.Builder();

        HttpClient httpClient = mock(HttpClient.class);
        HttpParams httpParams = mock(HttpParams.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);

        when(httpClient.execute(any(HttpRequestBase.class))).thenReturn(response);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.getParams()).thenReturn(httpParams);

        HTTPClientConfig conf = b.withUrl("http://www.google.com/notriak").withHttpClient(httpClient).withMapreducePath("/notAPath").withMaxConnctions(200).withTimeout(9000).build();

        HTTPClientAdapter client = (HTTPClientAdapter) HTTPRiakClientFactory.getInstance().newClient(conf);

        verify(httpParams).setIntParameter(AllClientPNames.CONNECTION_TIMEOUT, 9000);
        verify(httpParams).setIntParameter(AllClientPNames.SO_TIMEOUT, 9000);

        client.delete("b", "k");

        verify(httpClient).execute(any(HttpDelete.class));
    }

}
