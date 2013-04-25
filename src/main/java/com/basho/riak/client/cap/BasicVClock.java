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
package com.basho.riak.client.cap;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 *
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public class BasicVClock implements VClock
{
    private final byte[] value;
    
    /**
     * Create BasicVclock
     * @param value the vector clock bytes. NOTE: copies the value
     * @throws IllegalArgumentException if <code>value</code> is null
     */
    public BasicVClock(final byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("VClock value cannot be null");
        }
        this.value = Arrays.copyOf(value, value.length);
    }
    
    @Override
    public byte[] getBytes()
    {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public String asString()
    {
        try
        {
            return new String(value, "UTF-8");
        }
        catch (UnsupportedEncodingException ex)
        {
            throw new IllegalStateException(ex);
        }
    }
    
}
