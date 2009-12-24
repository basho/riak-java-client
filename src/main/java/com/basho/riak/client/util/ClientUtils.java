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
package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.json.JSONArray;
import org.json.JSONObject;

import com.basho.riak.client.RiakConfig;

/**
 * General utility functions.
 */
public class ClientUtils {

    // Matches the scheme, host and port of a URL
    private static String URL_PATH_MASK = "^(?:[A-Za-z0-9+-\\.]+://)?[^/]*";

    /**
     * Construct a new {@link HttpClient} instance given a {@link RiakConfig}.
     * 
     * @param config
     *            {@link RiakConfig} containing HttpClient configuration
     *            specifics.
     * @return A new {@link HttpClient}
     */
    public static HttpClient newHttpClient(RiakConfig config) {

        HttpClient http = config.getHttpClient();
        HttpConnectionManager m;

        if (http == null) {
            m = new MultiThreadedHttpConnectionManager();
            http = new HttpClient(m);
        } else {
            m = http.getHttpConnectionManager();
        }

        HttpConnectionManagerParams mp = m.getParams();
        if (config.getMaxConnections() != null) {
            mp.setIntParameter(HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS, config.getMaxConnections());
        }

        HttpClientParams cp = http.getParams();
        if (config.getTimeout() != null) {
            cp.setLongParameter(HttpClientParams.CONNECTION_MANAGER_TIMEOUT, config.getTimeout());
        }

        return http;
    }

    /**
     * Return a URL to the given bucket
     * 
     * @param config
     *            RiakConfig containing the base URL to Riak
     * @param bucket
     *            Bucket whose URL to retrieving
     * @return URL to the bucket
     */
    public static String makeURI(RiakConfig config, String bucket) {
        return config.getUrl() + "/" + urlEncode(bucket);
    }

    /**
     * Return a URL to the given object
     * 
     * @param config
     *            RiakConfig containing the base URL to Riak
     * @param bucket
     *            Bucket of the object
     * @param key
     *            Key of the object
     * @return URL to the object
     */
    public static String makeURI(RiakConfig config, String bucket, String key) {
        return makeURI(config, bucket) + "/" + urlEncode(key);
    }

    /**
     * Return a URL to the given object
     * 
     * @param config
     *            RiakConfig containing the base URL to Riak
     * @param bucket
     *            Bucket of the object
     * @param key
     *            Key of the object
     * @param extra
     *            Extra path information beyond the bucket and key (e.g. for
     *            link walking or query parameters)
     * @return URL to the object
     */
    public static String makeURI(RiakConfig config, String bucket, String key, String extra) {
        if (extra == null)
            return makeURI(config, bucket, key);

        if (!extra.startsWith("?") && !extra.startsWith("/")) {
            extra = "/" + extra;
        }

        return makeURI(config, bucket, key) + extra;
    }

    /**
     * @return Just the path portion of the given URL
     */
    public static String getPathFromUrl(String url) {
        return url.replaceFirst(URL_PATH_MASK, "");
    }

    /**
     * @return Just the path portion of the given URL
     */
    public static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException unreached) {
            // UTF-8 must be supported by every Java implementation:
            // http://java.sun.com/j2se/1.4.2/docs/api/java/nio/charset/Charset.html
            throw new IllegalStateException("UTF-8 must be supported", unreached);
        }
    }

    /**
     * Unquote and unescape an HTTP <code>quoted-string</code>:
     * 
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2
     * 
     * Does nothing if <code>s</code> is not quoted.
     * 
     * @param s
     *            <code>quoted-string</code> to unquote
     * @return s with quotes and backslash-escaped characters unescaped
     */
    public static String unquoteString(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s.replaceAll("\\\\(.)", "$1");
    }

    /**
     * Convert a header array returned from {@link HttpClient} to a map
     * 
     * @param headers
     *            Header array returned from HttpClient
     * @return Map of the header names to values
     */
    public static Map<String, String> asHeaderMap(Header[] headers) {
        Map<String, String> m = new HashMap<String, String>();
        for (Header header : headers) {
            m.put(header.getName().toLowerCase(), header.getValue());
        }
        return m;
    }

    /**
     * Convert a {@link JSONObject} to a map
     * 
     * @param json
     *            {@link JSONObject} to convert
     * @return Map of the field names to string representations of the values
     */
    public static Map<String, String> jsonObjectAsMap(JSONObject json) {
        if (json == null)
            return null;

        Map<String, String> m = new HashMap<String, String>();
        for (@SuppressWarnings("unchecked")
        Iterator iter = json.keys(); iter.hasNext();) {
            Object obj = iter.next();
            if (obj != null) {
                String key = obj.toString();
                m.put(key, json.optString(key));
            }
        }
        return m;
    }

    /**
     * Convert a {@link JSONArray} to a list
     * 
     * @param json
     *            {@link JSONArray} to convert
     * @return List of string representations of the elements
     */
    public static List<String> jsonArrayAsList(JSONArray json) {
        if (json == null)
            return null;

        List<String> l = new ArrayList<String>();
        for (int i = 0; i < json.length(); i++) {
            l.add(json.optString(i));
        }
        return l;
    }

    /**
     * Copies data from an {@link InputStream} to an {@link OutputStream} in
     * blocks
     * 
     * @param in
     *            InputStream to copy
     * @param out
     *            OutputStream to copy to
     * @throws IOException
     */
    public static void copyStream(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        while (true) {
            final int readCount = in.read(buffer);
            if (readCount == -1) {
                break;
            }
            out.write(buffer, 0, readCount);
        }
    }
}
