/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.util.http;

import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.CharsetUtils;
import io.netty.handler.codec.http.HttpHeaders;
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
 * @author Unknown
 * @since 1.0
 */
public class HttpMultipart
{

    private static byte[] HEADER_DELIM = CharsetUtils.utf8StringToBytes("\r\n\r\n");

    private static int indexOf(byte[] text, byte[] pattern, int fromIndex)
    {
        if (fromIndex >= text.length || fromIndex < 0)
        {
            throw new IllegalArgumentException("index not within range");
        }

        if (pattern.length == 0)
        {
            throw new IllegalArgumentException("pattern must not be empty");
        }

        byte first = pattern[0];
        int max = text.length - pattern.length;

        for (int i = fromIndex; i <= max; i++)
        {
            if (text[i] != first)
            {
                while (i <= max && text[i] != first)
                {
                    i++;
                }
            }

            if (i <= max)
            {
                int j = i + 1;
                int end = j + pattern.length - 1;
                for (int k = 1; j < end && text[j] == pattern[k]; j++, k++);
                if (j == end)
                {
                    return i;
                }
            }
        }
        return -1;
    }

    public static List<HttpMultipart.Part> parse(HttpHeaders headers, byte[] content)
    {
        if (!(content.length >= 2 && content[0] == '\r' && content[1] == '\n'))
        {
            // In order to parse the multipart efficiently, we want to treat the
            // first boundary identically to the others, so make sure that the
            // first boundary is preceded by a '\r\n' like the others
            byte[] newContent = new byte[content.length + 2];
            newContent[0] = '\r';
            newContent[1] = '\n';
            System.arraycopy(content, 0, newContent, 2, content.length);
            content = newContent;
        }

        String boundary = "\r\n--" + getBoundary(headers.get(Constants.HDR_CONTENT_TYPE));
        byte[] boundaryBytes = CharsetUtils.asBytes(boundary, CharsetUtils.ISO_8859_1);
        int boundarySize = boundary.length();
        if ("\r\n--".equals(boundary))
        {
            return null;
        }

        // While this parsing could be more efficiently done in one pass with a
        // hand written FSM, hopefully this method is more readable/intuitive.
        List<Part> parts = new ArrayList<Part>();
        int pos = indexOf(content, boundaryBytes, 0);
        if (pos != -1)
        {
            while (pos < content.length)
            {
                // first char of part
                int start = pos + boundarySize;
                // last char of part + 1
                int end = indexOf(content, boundaryBytes, start);
                // end of header section + 1
                int headerEnd = indexOf(content, HEADER_DELIM, pos);
                // start of body section
                int bodyStart = headerEnd + HEADER_DELIM.length;

                // check for end boundary, which is (boundary + "--")
                if (content.length >= (start + 2) && content[start] == '-' && content[start + 1] == '-')
                {
                    break;
                }

                if (end == -1)
                {
                    end = content.length;
                }

                if (headerEnd == -1)
                {
                    headerEnd = content.length;
                    bodyStart = end;
                }

                if (bodyStart > end)
                {
                    bodyStart = end;
                }

                Map<String, String> partHeaders =
                    parseHeaders(CharsetUtils.asASCIIString(Arrays.copyOfRange(content, start, headerEnd)));
                parts.add(new Part(partHeaders, Arrays.copyOfRange(content, bodyStart, end)));

                pos = end;
            }
        }

        return parts;
    }

    /**
     * Parse a block of header lines as defined here:
     *
     * http://tools.ietf.org/html/rfc822#section-3.2
     *
     * @param s The header blob
     * @return Map of header names to values
     */
    public static Map<String, String> parseHeaders(String s)
    {
        // "unfold" header lines (http://tools.ietf.org/html/rfc822#section-3.1)
        s = s.replaceAll("\r\n\\s+", " ");

        String[] headers = s.split("\r\n");
        Map<String, String> parsedHeaders = new HashMap<String, String>();
        for (String header : headers)
        {
            // Split header line into name and value
            String[] nv = header.split("\\s*:\\s*", 2);
            if (nv.length > 1)
            {
                parsedHeaders.put(nv[0].trim().toLowerCase(), nv[1].trim());
            }
        }
        return parsedHeaders;
    }

    /**
     * Given a content type value, get the "boundary" parameter
     *
     * @param contentType Content type value with boundary parameter. Should be
     * of the form "type/subtype; boundary=foobar; param=value"
     * @return Value of the boundary parameter
     */
    public static String getBoundary(String contentType)
    {
        String[] params = contentType.split("\\s*;\\s*");
        for (String param : params)
        {
            String[] nv = param.split("\\s*=\\s*", 2);
            if (nv.length > 1)
            {
                if ("boundary".equals(nv[0].toLowerCase()))
                {
                    return HttpParseUtils.unquoteString(nv[1]);
                }
            }
        }
        return "";
    }

    /**
     * A single part of a multipart entity
     */
    public static class Part
    {

        private Map<String, String> headers;
        private byte[] body = null;
        private InputStream stream;

        public Part(Map<String, String> headers, byte[] body)
        {
            this.headers = headers;
            if (body != null)
            {
                this.body = body.clone();
            }
        }

        public Part(Map<String, String> headers, InputStream body)
        {
            this.headers = headers;
            stream = body;
        }

        /**
         * Headers defined in the part
         */
        public Map<String, String> getHeaders()
        {
            return headers;
        }

        /**
         * Body of this part
         */
        public byte[] getBody()
        {
            if (body == null && stream != null)
            {
                try
                {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    for (int readCount = 0; readCount != -1; readCount = stream.read(buffer))
                    {
                        os.write(buffer, 0, readCount);
                    }
                    body = os.toByteArray();
                }
                catch (IOException e)
                { /* nop */

                }
                stream = null;
            }
            return body;
        }
    }
}
