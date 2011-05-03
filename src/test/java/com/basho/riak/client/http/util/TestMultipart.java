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
package com.basho.riak.client.http.util;

import org.junit.Test;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.http.util.Multipart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestMultipart {

    @Test public void null_result_if_not_multipart_message() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "text/plain");
        String body = "abc";
        assertNull(Multipart.parse(headers, body.getBytes()));
    }

    @Test public void null_result_if_not_content_type_missing_boundary_parameter() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed");
        String body = "\r\n--boundary\r\n" + "Content-Type: text/plain\r\n" + "\r\n" + "subpart\r\n" + "--boundary--";

        assertNull(Multipart.parse(headers, body.getBytes()));
    }

    @Test public void parses_multipart_with_1_empty_part() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body = "\r\n--boundary\r\n" + "\r\n" + "--boundary--";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(1, parts.size());
        assertEquals(0, parts.get(0).getHeaders().size());
        assertEquals("", parts.get(0).getBodyAsString());
    }

    @Test public void parses_multipart_with_1_part() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body = "\r\n--boundary\r\n" + "Content-Type: text/plain\r\n" + "\r\n" + "subpart\r\n" + "--boundary--";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(1, parts.size());
        assertEquals(1, parts.get(0).getHeaders().size());
        assertEquals("text/plain", parts.get(0).getHeaders().get(Constants.HDR_CONTENT_TYPE));
        assertEquals("subpart", parts.get(0).getBodyAsString());
    }

    @Test public void parses_multipart_with_invalid_utf8_binary_part() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");

        /* 00000000  0d 0a 2d 2d 62 6f 75 6e  64 61 72 79 0d 0a 43 6f |..--boundary..Co|
         * 00000010  6e 74 65 6e 74 2d 54 79  70 65 3a 20 74 65 78 74 |ntent-Type: text|
         * 00000020  2f 70 6c 61 69 6e 0d 0a  0d 0a f0 28 8c 28 0d 0a |/plain.....(.(..|
         * 00000030  2d 2d 62 6f 75 6e 64 61  72 79 2d 2d             |--boundary--|
         * 0000003c
        */
        byte[] body = new byte[]{
                13, 10, 45, 45, 98, 111, 117, 110, 100, 97, 114, 121, 13, 10, 67, 111,
                110, 116, 101, 110, 116, 45, 84, 121, 112, 101, 58, 32, 116, 101, 120,
                116, 47, 112, 108, 97, 105, 110, 13, 10, 13, 10,
                -16, 40, -116, 40, // <- invalid UTF8 sequence, valid binary data
                13, 10, 45, 45, 98, 111, 117, 110, 100, 97, 114, 121, 45, 45
        };

        byte[] invalidUtf8 = new byte[]{-16, 40, -116, 40};

        List<Multipart.Part> parts = Multipart.parse(headers, body);
        assertEquals(1, parts.size());
        assertEquals(1, parts.get(0).getHeaders().size());
        assertEquals("text/plain", parts.get(0).getHeaders().get(Constants.HDR_CONTENT_TYPE));
        assertArrayEquals(invalidUtf8, parts.get(0).getBody());
    }

    @Test public void parses_multipart_with_multiple_parts() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body = "\r\n--boundary\r\n" + "Content-Type: text/plain\r\n" + "\r\n" + "part1\r\n" + "--boundary\r\n" + "\r\n"
                      + "part2\r\n" + "--boundary--";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(2, parts.size());
        assertEquals("part1", parts.get(0).getBodyAsString());
        assertEquals("part2", parts.get(1).getBodyAsString());
    }

    @Test public void parses_multipart_subpart() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body = "\r\n--boundary\r\n" + "Content-Type: multipart/mixed; boundary=5hgaasMxj1NIcoxJBpWd4j9IuaW\r\n" + "\r\n"
                      + "--5hgaasMxj1NIcoxJBpWd4j9IuaW\r\n" + "Content-Type: application/octet-stream\r\n" + "\r\n"
                      + "subpart1\r\n" + "--5hgaasMxj1NIcoxJBpWd4j9IuaW\r\n" + "Content-Type: application/octet-stream\r\n"
                      + "\r\n" + "subpart2\r\n" + "--5hgaasMxj1NIcoxJBpWd4j9IuaW--\r\n" + "\r\n" + "--boundary--\r\n";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(1, parts.size());

        List<Multipart.Part> subparts = Multipart.parse(parts.get(0).getHeaders(), parts.get(0).getBody());
        assertEquals(2, subparts.size());
        assertEquals("subpart1", subparts.get(0).getBodyAsString());
        assertEquals("subpart2", subparts.get(1).getBodyAsString());
    }

    @Test public void parses_subpart_headers_and_body() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=\"boundary\"");
        String PART_BODY = "this part has multiple" + "lines of text\r\n";
        String body = "\r\n--boundary\r\n" + "X-Riak-Vclock: a85hYGBgzGDKBVIsLOLmazKYEhnzWBkmzFt4hC8LAA==\r\n" +
                      "Location: /riak/test/key\r\n" + "Content-Type: application/octet-stream\r\n" + "\r\n" + PART_BODY +
                      "\r\n--boundary--";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(3, parts.get(0).getHeaders().size());
        assertEquals("a85hYGBgzGDKBVIsLOLmazKYEhnzWBkmzFt4hC8LAA==",
                     parts.get(0).getHeaders().get(Constants.HDR_VCLOCK));
        assertEquals("/riak/test/key", parts.get(0).getHeaders().get(Constants.HDR_LOCATION));
        assertEquals("application/octet-stream", parts.get(0).getHeaders().get(Constants.HDR_CONTENT_TYPE));
        assertEquals(PART_BODY, parts.get(0).getBodyAsString());
    }

    @Test public void handles_quoted_boundary() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=\"\\\\\\x\\\"\""); // boundary
                                                                                  // is
                                                                                  // the
                                                                                  // string:
                                                                                  // \x"
        String body = "\r\n--\\x\"\r\n" + "\r\n" + "part\r\n" + "--\\x\"--";

        List<Multipart.Part> parts = Multipart.parse(headers, body.getBytes());
        assertEquals(1, parts.size());
        assertEquals(0, parts.get(0).getHeaders().size());
        assertEquals("part", parts.get(0).getBodyAsString());
    }
}
