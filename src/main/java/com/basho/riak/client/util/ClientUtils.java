/*
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.  
 */
package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import com.basho.riak.client.RiakConfig;

public class ClientUtils {

    private static String URL_PATH_MASK = "^(?:[A-Za-z0-9+-\\.]+://)?[^/]*"; 
    
    public static HttpClient newHttpClient(final RiakConfig config) {
        
        HttpClient http = config.getHttpClient();
        HttpConnectionManager m;
        
        if (http == null) {
            m = new MultiThreadedHttpConnectionManager();
            http = new HttpClient(m);
        } else {
            m = http.getHttpConnectionManager();
        }
            
        HttpConnectionManagerParams mp = m.getParams();
        if (config.getMaxConnections() != null)
            mp.setIntParameter(
                    HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS, config.getMaxConnections());

        HttpClientParams cp = http.getParams();
        if (config.getTimeout() != null)
            cp.setLongParameter(HttpClientParams.CONNECTION_MANAGER_TIMEOUT, config.getTimeout());

        return http;
    }

    public static String makeURI(final RiakConfig config, final String bucket) {
        return config.getUrl() + "/" + urlEncode(bucket);
    }

    public static String makeURI(final RiakConfig config, final String bucket,
            final String key) {
        return makeURI(config, bucket) + "/" + urlEncode(key);
    }

    public static String makeURI(final RiakConfig config, final String bucket,
            final String key, final String extra) {
        return makeURI(config, bucket, key) + "/" + extra;
    }
    
    public static String getPathFromUrl(String url) {
        return url.replaceFirst(URL_PATH_MASK, "");
    }

    public static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void copy(final InputStream in, final OutputStream out) throws IOException {
        final byte[] buffer = new byte[1024];
        while (true) {
            final int readCount = in.read(buffer);
            if (readCount == -1)
                break;
            out.write(buffer, 0, readCount);
        }
    }

    public static Map<String, String> asHeaderMap(Header[] headers) {
        Map<String, String> m = new HashMap<String, String>();
        for (Header header : headers) {
            m.put(header.getName().toLowerCase(), header.getValue());
        }
        return m;
    }
}
