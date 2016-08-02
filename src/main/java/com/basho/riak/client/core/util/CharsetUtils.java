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
package com.basho.riak.client.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils for dealing with {@code byte}, {@code String} charset
 * issues
 * 
 * This code is mainly from the Trifork fork of the original HTTP client and was written by
 * Krestan Krab and/or Erik Søe Sørensen.
 * 
 * @author Russell Brown <russelldb at basho dot com>
 */
public class CharsetUtils
{
    public static Charset ASCII = Charset.forName("ASCII");
    public static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
    public static Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * RegEx pattern to get the charset from a content-type value.
     */
    private static final Pattern CHARSET_PATT = Pattern.compile("\\bcharset *= *\"?([^ ;\"]+)\"?", Pattern.CASE_INSENSITIVE);

    /**
     * Attempts to parse the {@link Charset} from a contentType string.
     * 
     * If contentType is null or no charset declaration found, then UTF-8 is
     * returned. If the found Charset declaration is unknown on this platform
     * then a runtime exception is thrown.
     * 
     * @param contentType
     * @return a {@link Charset} parsed from a charset declaration in a
     *         {@code contentType} String.
     */
    public static Charset getCharset(String contentType)
    {
        if (contentType == null)
        {
            return UTF_8;
        }

        if (Constants.CTYPE_JSON_UTF8.equals(contentType))
        {
            return UTF_8; // Fast-track
        }

        Matcher matcher = CHARSET_PATT.matcher(contentType);
        if (matcher.find())
        {
            String encstr = matcher.group(1);

            if (encstr.equalsIgnoreCase("UTF-8"))
            {
                return UTF_8; // Fast-track
            }
            else
            {
                try
                {
                    return Charset.forName(encstr.toUpperCase());
                }
                catch (Exception e)
                {
                    // ignore //
                }
            }
        }

        return ISO_8859_1;
    }

    /**
     * Get the actual string value declared as the charset in a content-type
     * string, regardless of its validity.
     * <p>
     * NOTE: this is different from getCharset, which will always return a
     * default value.
     * </p>
     * 
     * @param contentType
     *            the content-type string
     * @return the verbatim charset declared or null if non-exists
     */
    public static String getDeclaredCharset(String contentType)
    {
        if (contentType == null)
        {
            return null;
        }

        Matcher matcher = CHARSET_PATT.matcher(contentType);
        if (matcher.find())
        {
            String encstr = matcher.group(1);
            return encstr;
        }
        else
        {
            return null;
        }
    }

    /**
     * Adds the utf-8 charset to a content type.
     * 
     * @param contentType
     * @return the {@code contentType} with {@literal ;charset=utf-8} appended.
     */
    public static String addUtf8Charset(String contentType)
    {
        if (contentType == null)
        {
            return "text/plain;charset=utf-8";
        }

        Matcher matcher = CHARSET_PATT.matcher(contentType);
        if (matcher.find())
        {
            // replace what ever content-type with utf8
            return contentType.substring(0, matcher.start(1)) + "utf-8" + contentType.substring(matcher.end(1));
        }

        return contentType + ";charset=utf-8";
    }
    
    /**
     * Adds the charset to a content type.
     * 
     * @param contentType
     * @return the {@code contentType} with {@code charset.name()} appended.
     */
    public static String addCharset(String charset, String contentType)
    {
        if (null == contentType)
        {
            return "text/plain;charset=" + charset;
        }
        else
        {
            Matcher matcher = CHARSET_PATT.matcher(contentType);
            if (matcher.find()) 
            {
                // replace what ever content-type with the charset
                return contentType.substring(0, matcher.start(1)) + 
                    charset + contentType.substring(matcher.end(1));
            }
            else
            {
                return contentType + ";charset=" + charset;
            }
        }
        
    }
    
    public static String addCharset(Charset charset, String contentType)
    {
        return addCharset(charset.name(), contentType);
    }
    
    /**
     * Turns a byte[] array into a string in the provided {@link Charset}
     * 
     * @param bytes
     * @param charset
     * @return a String
     */
    public static String asString(byte[] bytes, Charset charset)
    {
        if (bytes == null)
        {
            return null;
        }

        if (charset == null)
        {
            throw new IllegalArgumentException("Cannot get bytes without a Charset");
        }

        try
        {
            return new String(bytes, charset.name());
        }
        catch (UnsupportedEncodingException e)
        {
            throw new IllegalStateException(charset.name() + " must be present", e);
        }
    }
    
    /**
     * Turns a byte[] array into a UTF8 string
     * 
     * @param bytes
     * @return a String
     */
    public static String asUTF8String(byte[] bytes)
    {
        return asString(bytes, UTF_8);
    }
    
    public static String asASCIIString(byte[] bytes)
    {
        return asString(bytes, ASCII);
    }
    
    /**
     * Turn a string into an array of bytes using the passed {@link Charset}
     * 
     * @param string
     * @param charset
     * @return a byte[] array
     */
    public static byte[] asBytes(String string, Charset charset)
    {
        if (string == null)
        {
            return null;
        }

        if (charset == null)
        {
            throw new IllegalArgumentException("Cannot get bytes without a Charset");
        }

        try
        {
            return string.getBytes(charset.name());
        }
        catch (UnsupportedEncodingException e)
        {
            //since we are using *actual* charsets, not string lookups, this
            //should *never* happen. But it is better to throw it up than swallow it.
            throw new IllegalStateException("Charset present", e);
        }
    }

    /**
     * Turn a UTF-8 encoded string into an array of bytes
     * 
     * @param string
     * @return the bytes for the supplied String
     */
    public static byte[] utf8StringToBytes(String string)
    {
        return asBytes(string, UTF_8);
    }

    /**
     * Check if a content-type string has a charset field appended.
     * 
     * @param ctype
     *            the content-type string
     * @return true if {@code ctype} has a charset, false otherwise
     */
    public static boolean hasCharset(String ctype)
    {
        if (ctype == null)
        {
            return false;
        }
        Matcher matcher = CHARSET_PATT.matcher(ctype);
        if (matcher.find())
        {
            return true;
        }
        else
        {
            return false;
        }
    }
}
