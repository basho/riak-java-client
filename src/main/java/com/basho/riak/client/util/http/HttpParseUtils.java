/*
 * Copyright 2013 Basho Technologies Inc.
 *
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

/**
 * @author Russell Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 */
public class HttpParseUtils
{
    /**
     * Unquote and unescape an HTTP <code>quoted-string</code>:
     * 
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2
     * 
     * Does nothing if {@code s} is not quoted.
     * 
     * @param s
     *            {@code quoted-string} to unquote
     * @return s with quotes and backslash-escaped characters unescaped
     */
    public static String unquoteString(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s.replaceAll("\\\\(.)", "$1");
    }
}
