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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a multipart entity as described here:
 * 
 * http://tools.ietf.org/html/rfc2046#section-5.1
 */
public class Multipart {

    private static final byte[] HEADER_DELIM = {'\r','\n','\r','\n'};
    private static final byte[] CR_NL = {'\r','\n'};
    private static final byte[] CR_NL_DASH_DASH = {'\r','\n','-','-'};
    private static final byte[] DASH_DASH = {'-','-'};
    
    /**
     * Parses a multipart message or a multipart subpart of a multipart message.
     * 
     * @return A list of the parts parsed into headers and body of this
     *         multipart message
     */
    public static List<Multipart.Part> parse(Map<String, String> headers, byte[] body) {
        if (headers == null || body == null ||  body.length == 0)
            return null;

        if (!startsWith(body, CR_NL, 0)) {
            // In order to parse the multipart efficiently, we want to treat the
            // first boundary identically to the others, so make sure that the
            // first boundary is preceded by a '\r\n' like the others
            body = ByteUtils.concat(CR_NL, body);
        }

        byte[] boundary = ByteUtils.concat( CR_NL_DASH_DASH, getBoundary(headers.get(Constants.HDR_CONTENT_TYPE)).getBytes(ClientUtils.ISO_8859_1));
        int boundarySize = boundary.length;
        if (Arrays.equals(CR_NL_DASH_DASH, boundary))
            return null;

        // While this parsing could be more efficiently done in one pass with a
        // hand written FSM, hopefully this method is more readable/intuitive.
        List<Part> parts = new ArrayList<Part>();
        int pos = indexOf(body, boundary, 0);
        if (pos != -1) {
            while (pos < body.length) {
                // first char of part
                int start = pos + boundarySize;
                // last char of part + 1
                int end = indexOf(body, boundary, start);
                // end of header section + 1
                int headerEnd = indexOf(body, HEADER_DELIM, pos+1);
                // start of body section
                int bodyStart = headerEnd + HEADER_DELIM.length;

                // check for end boundary, which is (boundary + "--")
                if (startsWith(body, DASH_DASH, start)) {
                    break;
                }

                if (end == -1) {
                    end = body.length;
                }

                if (headerEnd == -1) {
                    headerEnd = body.length;
                    bodyStart = end;
                }

                if (bodyStart > end) {
                    bodyStart = end;
                }

                Map<String, String> partHeaders = parseHeaders(new String(body, start, headerEnd-start, ClientUtils.ISO_8859_1));
                byte[] partBody = getBytes(body,bodyStart, end);
                parts.add(new Part(partHeaders, partBody));

                pos = end;
            }
        }

        return parts;
    }

	private static boolean startsWith(byte[] body, byte[] key, int start) {
		for( int i = 0; i<key.length; i++ ) {
			if( body[start+i] != key[i] ) return false;
		}
		return true;
	}

	private static int indexOf(byte[] bytes, byte[] headerDelim, int from) {
		boolean found = false;
    	for( int outer = from; outer<bytes.length; outer++) {
    		if( bytes[outer] == headerDelim[0] ) {
    			for( int inner = 1; inner<headerDelim.length; inner++) {
    				if( (outer+inner+1)>bytes.length || bytes[outer+inner] != headerDelim[inner] ) {
    					found = false;
    					break;
    				}
    				found = true;
    			}
    			if(found) return outer;
    		}
    	}
    	return -1;
    }

	private static byte[] getBytes(byte[] body, int from, int to) {
		return Arrays.copyOfRange(body, from, to);
	}

	/**
     * Parse a block of header lines as defined here:
     * 
     * http://tools.ietf.org/html/rfc822#section-3.2
     * 
     * @param s
     *            The header blob
     * @return Map of header names to values
     */
    public static Map<String, String> parseHeaders(String s) {
        // "unfold" header lines (http://tools.ietf.org/html/rfc822#section-3.1)
        s.replaceAll("\r\n\\s+", " ");

        String[] headers = s.split("\r\n");
        Map<String, String> parsedHeaders = new HashMap<String, String>();
        for (String header : headers) {
            // Split header line into name and value
            String[] nv = header.split("\\s*:\\s*", 2);
            if (nv.length > 1) {
                parsedHeaders.put(nv[0].trim().toLowerCase(), nv[1].trim());
            }
        }
        return parsedHeaders;
    }

    /**
     * Given a content type value, get the "boundary" parameter
     * 
     * @param contentType
     *            Content type value with boundary parameter. Should be of the
     *            form "type/subtype; boundary=foobar; param=value"
     * @return Value of the boundary parameter
     */
    public static String getBoundary(String contentType) {
        String[] params = contentType.split("\\s*;\\s*");
        for (String param : params) {
            String[] nv = param.split("\\s*=\\s*", 2);
            if (nv.length > 1) {
                if ("boundary".equals(nv[0].toLowerCase()))
                    return ClientUtils.unquoteString(nv[1]);
            }
        }
        return "";
    }

    /**
     * A single part of a multipart entity
     */
    public static class Part {
        private Map<String, String> headers;
        private byte[] body;
        private InputStream stream;

        public Part(Map<String, String> headers, byte[] body) {
            this.headers = headers;
            this.body = body;
        }

        public Part(Map<String, String> headers, InputStream body) {
            this.headers = headers;
            stream = body;
        }

        /**
         * Headers defined in the part
         */
        public Map<String, String> getHeaders() {
            return headers;
        }

        /**
         * Body of this part
         */
        public byte[] getBody() {
            if (body == null && stream != null) {
                try {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    for (int readCount = 0; readCount != -1; readCount = stream.read(buffer)) {
                        os.write(buffer, 0, readCount);
                    }
                    body = os.toByteArray();
                } catch (IOException e) { /* nop */}
                stream = null;
            }
            return body;
        }
        
        public String getBodyAsString() {
           byte[] body = getBody();
           if (body == null)
              return null;
           return new String(body, ClientUtils.getCharset(headers));
        }

        public InputStream getStream() {
            if (stream == null && body != null) {
                stream = new ByteArrayInputStream(body);
            }

            return stream;
        }
    }
}