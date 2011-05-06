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

import static com.basho.riak.client.util.CharsetUtils.utf8StringToBytes;
import static org.junit.Assert.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.OneTokenInputStream;

public class TestOneTokenInputStream {

    OneTokenInputStream impl;
    
    @Test public void stream_ends_at_delimiter_in_middle_of_stream() throws IOException {
        String delim = "\r\n--boundary";
        String part1 = "abcdefghijklmnop";
        String part2 = "qrstuvwxyz";
        String body = 
            part1 +
    		delim + "\r\n" +
    		part2;
        InputStream stream = new ByteArrayInputStream(utf8StringToBytes(body));
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }

    @Test public void no_content_if_stream_starts_with_delimiter() throws IOException {
        String delim = "\r\n--boundary";
        String body = delim + "abcdef";
        InputStream stream = new ByteArrayInputStream(utf8StringToBytes(body));
        impl = new OneTokenInputStream(stream, delim);
        
        assertEquals(-1, impl.read());
    }

    @Test public void stream_ends_at_delimiter_at_end_of_stream() throws IOException {
        String delim = "\r\n--boundary";
        String part1 = "abcdefghijklmnop";
        String body = part1 + delim;
        InputStream stream = new ByteArrayInputStream(utf8StringToBytes(body));
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }

    @Test public void stream_ends_at_end_of_stream_when_no_delimiter() throws IOException {
        String delim = "\r\n--boundary";
        String part1 = "abcdefghijklmnop";
        String body = part1;
        InputStream stream = new ByteArrayInputStream(utf8StringToBytes(body));
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }
}
