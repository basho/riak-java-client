/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.RiakMessage;


public class Operations
{

    public static void checkPBMessageType(RiakMessage msg, byte expected)
    {
        byte pbMessageCode = msg.getCode();
        if (pbMessageCode != expected)
        {
            final short unsignedBytePbMessageCode = getUnsignedByteValue(pbMessageCode);
            throw new IllegalStateException("Wrong response; expected "
                + expected
                + " received " + unsignedBytePbMessageCode);
        }

    }

    /**
     * Convert a Java signed byte to unsigned.
     * Returns the unsigned value as a (signed) short.
     * @param b a java signed byte
     * @return a short containing the converted value.
     */
    public static short getUnsignedByteValue(byte b)
    {
        return (short)(b & 0xFF);
    }

    /**
     * Convert a Java signed int to unsigned.
     * Returns the unsigned value as a (signed) long.
     * @param i a java signed int
     * @return a long containing the converted value.
     */
    public static long getUnsignedIntValue(int i)
    {
        return i & 0x00000000ffffffffL;
    }
}
