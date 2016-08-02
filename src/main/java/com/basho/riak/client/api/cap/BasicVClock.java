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
package com.basho.riak.client.api.cap;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Encapsulates a Riak vector clock.
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public class BasicVClock implements VClock
{
    private final byte[] value;
    
    /**
     * Create a BasicVclock.
     * @param value the vector clock bytes. NOTE: copies the value
     * @throws IllegalArgumentException if <code>value</code> is null
     */
    public BasicVClock(final byte[] value)
    {
        if (value == null)
        {
            throw new IllegalArgumentException("VClock value cannot be null");
        }
        this.value = Arrays.copyOf(value, value.length);
    }
    
    /**
     * Create a BasicVclock from utf8 String.
     * @param vclock the vector clock.
     * @throws IllegalArgumentException if {@code vclock} is null
     */
    public BasicVClock(String vclock)
    {
        if (vclock == null)
        {
            throw new IllegalArgumentException("VClock value cannot be null");
        }
        this.value = vclock.getBytes(Charset.forName("UTF-8"));
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
    
    @Override
    public boolean equals(Object o)
    {
        if (o != null)
        {
            if (o instanceof VClock)
            {
                VClock other = (VClock)o;
                return Arrays.equals(other.getBytes(), value);
            }
        }
            
        return false;
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 67 * hash + Arrays.hashCode(this.value);
        return hash;
    }
}
