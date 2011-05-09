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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.http.util.Constants;

/**
 * @author russell
 * 
 */
public class CharsetUtilsTest {

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @After public void tearDown() throws Exception {}

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#getCharset(java.util.Map)}
     * .
     */
    @Test public void getCharsetFromHeaders() {
        final Map<String, String> headers = new HashMap<String, String>();
        headers.put(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON_UTF8);
        assertEquals(Charset.forName("UTF-8"), CharsetUtils.getCharset(headers));

        headers.put(Constants.HDR_CONTENT_TYPE, "utter_tripe");
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset(headers));

        headers.put(Constants.HDR_CONTENT_TYPE, null);
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset(headers));

        headers.remove(Constants.HDR_CONTENT_TYPE);
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset(headers));

        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset((Map<String, String>) null));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#getCharset(java.lang.String)}
     * .
     */
    @Test public void getCharsetFromContentType() {
        assertEquals(Charset.forName("UTF-8"), CharsetUtils.getCharset(Constants.CTYPE_TEXT_UTF8));
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset("text/plain;charset=NotACharSet"));
        assertEquals(Charset.forName("UTF-16"), CharsetUtils.getCharset("text/plain;charset=UTF-16"));
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset("gibberish"));
        assertEquals(Charset.forName("ISO8859_1"), CharsetUtils.getCharset((String) null));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#addUtf8Charset(java.lang.String)}
     * .
     */
    @Test public void addUTF8CharsetToContentType() {
        assertTrue("Expceted UTF-8 charset to be added to content type",
                   Constants.CTYPE_JSON_UTF8.equalsIgnoreCase(CharsetUtils.addUtf8Charset(Constants.CTYPE_JSON)));

        // null gets a default
        assertEquals("text/plain;charset=utf-8", CharsetUtils.addUtf8Charset(null));

        // charset gets replaced
        assertEquals("text/plain;charset=utf-8", CharsetUtils.addUtf8Charset("text/plain;charset=utf-16"));

        // nonsense is untouched
        assertEquals("nonsense;charset=utf-8", CharsetUtils.addUtf8Charset("nonsense"));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#asBytes(java.lang.String, java.nio.charset.Charset)}
     * .
     */
    @Test public void stringToBytes() throws Exception {
        String example = "example";
        byte[] b = example.getBytes("UTF-8");

        assertArrayEquals(b, CharsetUtils.asBytes(example, Charset.forName("UTF-8")));

        b = example.getBytes("UTF-16");
        assertArrayEquals(b, CharsetUtils.asBytes(example, Charset.forName("UTF-16")));

        b = example.getBytes("ISO8859_1");
        assertArrayEquals(b, CharsetUtils.asBytes(example, Charset.forName("ISO8859_1")));

        assertNull(CharsetUtils.asBytes(null, Charset.forName("UTf-8")));

        try {
            CharsetUtils.asBytes(example, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // NO-OP
        }
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#utf8StringToBytes(java.lang.String)}
     * .
     */
    @Test public void utf8StringToBytes() throws Exception {
        String example = "example";
        byte[] b = example.getBytes("UTF-8");

        assertArrayEquals(b, CharsetUtils.utf8StringToBytes(example));
        assertNull(CharsetUtils.utf8StringToBytes(null));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#asString(byte[], java.nio.charset.Charset)}
     * .
     */
    @Test public void bytesToString() throws Exception {
        String example = "example";
        byte[] b = example.getBytes("UTF-8");

        assertEquals(example,CharsetUtils.asString(b, Charset.forName("UTF-8")));

        b = example.getBytes("UTF-16");
        assertEquals(example, CharsetUtils.asString(b, Charset.forName("UTF-16")));

        b = example.getBytes("ISO8859_1");
        assertEquals(example, CharsetUtils.asString(b, Charset.forName("ISO8859_1")));

        assertNull(CharsetUtils.asString(null, Charset.forName("UTf-8")));

        try {
            CharsetUtils.asString(b, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // NO-OP
        }
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.util.CharsetUtils#asUTF8String(byte[])}.
     */
    @Test public void bytesToUtf8String() throws Exception {
        String example = "example";
        byte[] b = example.getBytes("UTF-8");

        assertEquals(example, CharsetUtils.asUTF8String(b));
        assertNull(CharsetUtils.asUTF8String(null));
    }

}
