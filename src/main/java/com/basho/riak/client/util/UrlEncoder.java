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
package com.basho.riak.client.util;

/**
 * Url encoder that takes a byte array as an argument.
 * 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class UrlEncoder
{
    private static final char[] hex =  { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F' };
    
    public static String urlEncode(byte[] bytesToEncode)
    {
        StringBuilder sb = new StringBuilder(bytesToEncode.length);
        
        for (int i = 0; i < bytesToEncode.length; i++)
        {
            char c = (char)bytesToEncode[i];
            switch(c)
            {
                case ' ':
                    sb.append(c);
                    break;
                case '/':
                case '*':
                case '-':
                case '_':
                case '.':
                    sb.append(c);
                    break;
                default:
                    if ( (c >= 'a' && c <= 'z') ||
                         (c >= 'A' && c <= 'Z') ||
                         (c >= '0' && c <= '9') )
                    {
                        sb.append(c);
                    }
                    else
                    {
                        sb.append('%');
                        sb.append(hex[(c & 0xF0) >> 4]);
                        sb.append(hex[c & 0x0F]);
                    }
            }
        }
        return sb.toString();
    }
}
