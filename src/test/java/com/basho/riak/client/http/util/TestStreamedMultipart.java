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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.StreamedMultipart;
import com.basho.riak.client.http.util.Multipart.Part;

public class TestStreamedMultipart {
    
    @SuppressWarnings("serial") final Map<String, String> headers = new HashMap<String, String>() {{
        put("content-type", "multipart/mixed; boundary=\"boundary\"");
    }};
    final String body = 
        "\r\n" +
        "--boundary\r\n" +
        "X-Riak-Vclock: a85hYGBgzGDKBVIsLOLmazKYEhnzWBkmzFt4hC8LAA==\r\n" +
        "Location: /riak/test/key\r\n" +
        "Content-Type: application/octet-stream\r\n" +
        "\r\n" +
        "foo\r\n" +
        "--boundary\r\n" +
        "Content-Type: text/plain\r\n" +
        "Header: value1\r\n" +
        "\r\n" +
        "multiple lines of\n" +
        "text in this part\n" +
        "\r\n" +
        "--boundary with some trailing text on the same line\r\n" +
        "Content-Type: text/csv\r\n" +
        "Header: value2\r\n" +
        "\r\n" +
        "baz,\r\n" +
        "--boundary--\r\n" +
        "postlude\n" +
        "\r\n";
    InputStream stream = new ByteArrayInputStream(body.getBytes());
        
    StreamedMultipart impl;
    
    @Test public void no_constructor_exception_on_valid_input() throws EOFException, IOException {
        String body = 
            "preamble\r\n" +
            "--boundary\r\n" +
            "message\r\n" +
            "--boundary--";
        stream = new ByteArrayInputStream(body.getBytes());
        
        new StreamedMultipart(headers, stream);
    }

    @Test public void no_constructor_exception_when_no_preamble() throws EOFException, IOException {
        String body = 
            "--boundary\r\n" +
            "message\r\n" +
            "--boundary--";
        stream = new ByteArrayInputStream(body.getBytes());
        
        new StreamedMultipart(headers, stream);
    }

    @Test (expected=EOFException.class) public void throws_when_initial_boundary_is_missing() throws EOFException, IOException {
        String body = 
            "--wrong\r\n" +
            "message\r\n" +
            "--wrong--";
        stream = new ByteArrayInputStream(body.getBytes());
        
        new StreamedMultipart(headers, stream);
    }
    
    @Test public void finds_all_parts() throws EOFException, IOException {
        impl = new StreamedMultipart(headers, stream);
        assertNotNull(impl.next());
        assertNotNull(impl.next());
        assertNotNull(impl.next());
        assertNull(impl.next());
    }

    @Test public void correctly_parses_part_headers() throws EOFException, IOException {
        Map<String, String> partHeaders;
        impl = new StreamedMultipart(headers, stream);

        partHeaders = impl.next().getHeaders();
        assertEquals(3, partHeaders.keySet().size());
        assertEquals("a85hYGBgzGDKBVIsLOLmazKYEhnzWBkmzFt4hC8LAA==", partHeaders.get("x-riak-vclock"));
        assertEquals("/riak/test/key", partHeaders.get("location"));
        assertEquals("application/octet-stream", partHeaders.get("content-type"));

        partHeaders = impl.next().getHeaders();
        assertEquals(2, partHeaders.keySet().size());
        assertEquals("text/plain", partHeaders.get("content-type"));
        assertEquals("value1", partHeaders.get("header"));

        partHeaders = impl.next().getHeaders();
        assertEquals(2, partHeaders.keySet().size());
        assertEquals("text/csv", partHeaders.get("content-type"));
        assertEquals("value2", partHeaders.get("header"));
        
        assertNull(impl.next());
    }

    @Test public void correctly_positions_body_stream_when_all_part_bodies_are_consumed() throws EOFException, IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        impl = new StreamedMultipart(headers, stream);

        os.reset();
        ClientUtils.copyStream(impl.next().getStream(), os);
        assertEquals(os.toString(), "foo");

        os.reset();
        ClientUtils.copyStream(impl.next().getStream(), os);
        assertEquals("multiple lines of\ntext in this part\n", os.toString());

        os.reset();
        ClientUtils.copyStream(impl.next().getStream(), os);
        assertEquals("baz,", os.toString());

        assertNull(impl.next());
    }
    
    @Test public void correctly_positions_body_stream_when_parts_are_skipped() throws EOFException, IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        impl = new StreamedMultipart(headers, stream);

        Part[] parts = new Part[] { impl.next(), impl.next(), impl.next() };
        assertNull(impl.next());

        os.reset();
        ClientUtils.copyStream(parts[2].getStream(), os);
        assertEquals("baz,", os.toString());

        os.reset();
        ClientUtils.copyStream(parts[1].getStream(), os);
        assertEquals("multiple lines of\ntext in this part\n", os.toString());

        os.reset();
        ClientUtils.copyStream(parts[0].getStream(), os);
        assertEquals("foo", os.toString());

    }
}
