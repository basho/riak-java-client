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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils for dealing with byte[], String charset issues, especially since Java 5
 * is less cool than Java 6 in this respect.
 *
 * This code is all from the Trifork fork of the client and was written by
 * Krestan Krab, Christian Hvitved and Erik Søe Sørensen.
 *
 * @author russell
 * 
 */
public class CharsetUtils {
    public static Charset ASCII = Charset.forName("ASCII");
    public static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
    public static Charset UTF_8 = Charset.forName("UTF-8");

    public static Charset getCharset(Map<String, String> headers) {
        return getCharset(headers.get(com.basho.riak.client.http.util.Constants.HDR_CONTENT_TYPE));
    }

    static Pattern CHARSET_PATT = Pattern.compile("\\bcharset *= *\"?([^ ;\"]+)\"?", Pattern.CASE_INSENSITIVE);

    /**
     * Attempts to parse the {@link Charset} from a contentType string.
     * 
     * If contentType is null or no charset declaration found, then UTF-8 is returned.
     * If the found Charset declaration is unknown on this platform then a runtime exception is thrown.
     * @param contentType
     * @return a {@link Charset} parsed from a charset declaration in a contentType strng.
     */
    public static Charset getCharset(String contentType) {
        if (contentType == null) {
            return ISO_8859_1;
        }

        if (com.basho.riak.client.http.util.Constants.CTYPE_JSON_UTF8.equals(contentType)) {
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

    /**
     * Adds the utf-8 charset to a content type.
     * @param contentType
     * @return the contentType with ;charset=utf-8 appended.
     */
    public static String addUtf8Charset(String contentType) {
        if (contentType == null) {
            return "text/plain;charset=utf-8";
        }

        Matcher matcher = CHARSET_PATT.matcher(contentType);
        if (matcher.find()) {
            // replace what ever content-type with utf8
            return contentType.substring(0, matcher.start(1)) + "utf-8" + contentType.substring(matcher.end(1));
        }

        return contentType + ";charset=utf-8";
    }
    
    /**
     * Turns a byte[] array into a string in the provided {@link Charset}
     * @param bytes
     * @param charset
     * @return a String
     */
    public static String asString(byte[] bytes, Charset charset) {
        return charset.decode(ByteBuffer.wrap(bytes)).toString();
    }
    
    /**
     * Turns a byte[] array into a UTF8 string
     * @param bytes
     * @param charset
     * @return a String
     */
    public static String asUTF8String(byte[] bytes) {
        try {
            return new String(bytes, UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF8 must be present", e);
        }
    }
    
    /**
     * Turn a string into an array of bytes using the passed {@link Charset}
     * @param string
     * @param charset
     * @return a byte[] array
     */
    public static byte[] asBytes(String string, Charset charset) {
        try {
            return string.getBytes(charset.name());
        } catch (UnsupportedEncodingException e) {
            //since we are using *actual* charsets, not string lookups, this
            //should *never* happen. But it is better to throw it up than swallow it.
            throw new IllegalStateException("Charset present", e);
        }
    }

    /**
     * Turn a UTF-8 encoded string into an array of bytes
     * @param string
     * @return
     */
    public static byte[] utf8StringToBytes(String string) {
        return asBytes(string, UTF_8);
    }

}
