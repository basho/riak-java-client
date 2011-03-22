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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONArray;
import org.json.JSONObject;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.RiakExceptionHandler;

/**
 * Utility functions.
 */
public class ClientUtils {

    // Matches the scheme, host and port of a URL
    private static String URL_PATH_MASK = "^(?:[A-Za-z0-9+-\\.]+://)?[^/]*";
    private static Random rng = new Random();
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
        	mp.setMaxTotalConnections(config.getMaxConnections());
        	mp.setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION, config.getMaxConnections());
        }

        HttpClientParams cp = http.getParams();
        if (config.getTimeout() != null) {
            mp.setConnectionTimeout(config.getTimeout().intValue());
            cp.setConnectionManagerTimeout(config.getTimeout());
            cp.setSoTimeout(config.getTimeout().intValue());
        }
        if (config.getRetryHandler() != null) {
            cp.setParameter(HttpMethodParams.RETRY_HANDLER, config.getRetryHandler());
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
        if (key == null)
            return makeURI(config, bucket);
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
     * Return just the path portion of the given URL
     */
    public static String getPathFromUrl(String url) {
        if (url == null)
            return null;
        return url.replaceFirst(URL_PATH_MASK, "");
    }

    /**
     * UTF-8 encode the string
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
     * Decodes a UTF-8 encoded string
     */
    public static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException unreached) {
            throw new IllegalStateException("UTF-8 must be supported", unreached);
        }
    }

    /**
     * Base64 encodes the first 4 bytes of clientId into a value acceptable for
     * the X-Riak-ClientId header.
     */
    public static String encodeClientId(byte[] clientId) {
        if (clientId == null || clientId.length < 4)
            throw new IllegalArgumentException("ClientId must be at least 4 bytes");

        try {
            return new String(Base64.encodeBase64(new byte[] { clientId[0], clientId[1], clientId[2], clientId[3] }), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 support is required by JVM");
        }
    }

    public static String encodeClientId(String clientId) {
        return encodeClientId(clientId.getBytes());
    }

    /**
     * Returns a random X-Riak-ClientId header value.
     */
    public static String randomClientId() {
        byte[] rnd = new byte[4];
        rng.nextBytes(rnd);
        return encodeClientId(rnd);
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
        if (headers != null) {
            for (Header header : headers) {
                m.put(header.getName().toLowerCase(), header.getValue());
            }
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
        for (@SuppressWarnings("unchecked") Iterator iter = json.keys(); iter.hasNext();) {
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
     * Join the elements in arr in to a single string separated by delimiter.
     */
    public static String join(String[] arr, String delimiter) {
        StringBuffer buf = new StringBuffer();
        if (arr == null || arr.length == 0)
            return null;

        buf.append(arr[0]);
        for (int i = 1; i < arr.length; i++) {
        	buf.append(delimiter);
        	buf.append(arr[i]);
        }
        return buf.toString();
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

    /**
     * Parse a link header into a {@link RiakLink}. See {@link LinkHeader}.
     * 
     * @param header
     *            The HTTP Link header value.
     * @return List of {@link RiakLink} objects constructed from the links in
     *         header in order.
     */
    public static List<RiakLink> parseLinkHeader(String header) {
        List<RiakLink> links = new ArrayList<RiakLink>();
        Map<String, Map<String, String>> parsedLinks = LinkHeader.parse(header);
        for (Entry<String, Map<String, String>> e: parsedLinks.entrySet()) {
        	String url = e.getKey();
        	RiakLink link = parseOneLink(url, e.getValue());
        	if (link != null) {
        		links.add(link);
        	}
        }
        return links;
    }

    /**
     * Create a {@link RiakLink} object from a single parsed link from the Link
     * header
     * 
     * @param url
     *            The link URL
     * @param params
     *            The link parameters
     * @return {@link RiakLink} object
     */
    private static RiakLink parseOneLink(String url, Map<String, String> params) {
        String tag = params.get(Constants.LINK_TAG);
        if (tag != null) {
            String[] parts = url.split("/");
            if (parts.length >= 2)
                return new RiakLink(parts[parts.length - 2], parts[parts.length - 1], tag);
        }
        return null;
    }

    /**
     * Extract only the user-specified metadata headers from a header set: all
     * headers prefixed with X-Riak-Meta-. The prefix is removed before
     * returning.
     * 
     * @param headers
     *            The full HTTP header set from the response
     * @return Map of all headers prefixed with X-Riak-Meta- with prefix
     *         removed.
     */
    public static Map<String, String> parseUsermeta(Map<String, String> headers) {
        Map<String, String> usermeta = new HashMap<String, String>();
        if (headers != null) {
        	for (Entry<String, String> e : headers.entrySet()) { 
        		String header = e.getKey();
        		if (header != null && header.toLowerCase().startsWith(Constants.HDR_USERMETA_PREFIX)) {
        			usermeta.put(header.substring(Constants.HDR_USERMETA_PREFIX.length()), e.getValue());
        		}
        	}
        }
        return usermeta;
    }
    
    /**
     * Convert a multipart/mixed document to a list of {@link RiakObject}s.
     * 
     * @param riak
     *            {@link RiakClient} this object should be associate with, or
     *            null if none
     * @param bucket
     *            original object's bucket
     * @param key
     *            original object's key
     * @param docHeaders
     *            original document's headers
     * @param docBody
     *            original document's body
     * @return List of {@link RiakObject}s represented by the multipart document
     */
    public static List<RiakObject> parseMultipart(RiakClient riak, String bucket, String key,
                                                  Map<String, String> docHeaders, byte[] docBody) {

        String vclock = null;

        if (docHeaders != null) {
            vclock = docHeaders.get(Constants.HDR_VCLOCK);
        }

        List<Multipart.Part> parts = Multipart.parse(docHeaders, docBody);
        List<RiakObject> objects = new ArrayList<RiakObject>();
        if (parts != null) {
            for (Multipart.Part part : parts) {
                Map<String, String> headers = part.getHeaders();
                List<RiakLink> links = parseLinkHeader(headers.get(Constants.HDR_LINK));
                Map<String, String> usermeta = parseUsermeta(headers);
                String location = headers.get(Constants.HDR_LOCATION);
                String partBucket = bucket;
                String partKey = key;

                if (location != null) {
                    String[] locationParts = location.split("/");
                    if (locationParts.length >= 2) {
                        partBucket = locationParts[locationParts.length - 2];
                        partKey = locationParts[locationParts.length - 1];
                    }
                }

                RiakObject o = new RiakObject(riak, partBucket, partKey, part.getBody(),
                                              headers.get(Constants.HDR_CONTENT_TYPE), links, usermeta, vclock,
                                              headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));
                objects.add(o);
            }
        }
        return objects;
    }

    
    public static Charset ASCII = Charset.forName("ASCII");
    public static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
    public static Charset UTF_8 = Charset.forName("UTF-8");
    
    public static Charset getCharset(Map<String,String> headers) {
    	return getCharset(headers.get(Constants.HDR_CONTENT_TYPE));
    }
    
    static Pattern CHARSET_PATT = Pattern.compile("\\bcharset *= *\"?([^ ;\"]+)\"?",
                                                  Pattern.CASE_INSENSITIVE);
    
    public static Charset getCharset(String contentType) {
        if (contentType == null)
            return ISO_8859_1;
        
        if (Constants.CTYPE_JSON_UTF8.equals(contentType)) {
            return UTF_8; // Fast-track
        }
        
        Matcher matcher = CHARSET_PATT.matcher(contentType);
        if (matcher.find()) {
           String encstr = matcher.group(1);
        
           if (encstr.equalsIgnoreCase("UTF-8")) {
               return UTF_8; // Fast-track
           } else {
               try {
                   return Charset.forName(encstr.toUpperCase());
               } catch (Exception e) {
                   // ignore //
               }
           }
        }

        return ISO_8859_1;
    }

	public static String addUtf8Charset(String contentType) {
		if (contentType == null) {
			return "text/plain;charset=utf-8";
		}

		Matcher matcher = CHARSET_PATT.matcher(contentType);
		if (matcher.find()) {
			// replace what ever content-type with utf8
			return contentType.substring(0, matcher.start(1))
				+ "utf-8"
				+ contentType.substring(matcher.end(1));
		}

		return contentType+";charset=utf-8";
	}

    
    /**
     * Throws a checked {@link Exception} not declared in the method signature,
     * which can be particularly useful for throwing checked exceptions within a
     * {@link RiakExceptionHandler}. Clearly, this circumvents compiler
     * safeguards, so use with caution. You've been warned.
     * 
     * @param exception
     *            A checked (or unchecked) exception to be thrown.
     */
    public static void throwChecked(final Throwable exception) {
        new CheckedThrower<RuntimeException>().throwChecked(exception);
    }
}

class CheckedThrower<T extends Throwable> {
    @SuppressWarnings("unchecked") public void throwChecked(Throwable exception) throws T {
        throw (T) exception;
    }
}
