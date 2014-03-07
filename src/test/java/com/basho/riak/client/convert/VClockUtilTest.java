/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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
package com.basho.riak.client.convert;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class VClockUtilTest extends ConversionUtilTest
{

    @Test
    public void getVClockField()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();
        final VClock expected = new BasicVClock(clockBytes);
        pojo.vclock = expected;

        assertEquals(expected, VClockUtil.getVClock(pojo));
    }

    @Test
    public void getVClockMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        final VClock expected = new BasicVClock(clockBytes);
        pojo.setVClock(expected);
        assertEquals(expected, VClockUtil.getVClock(pojo));
    }
    
    @Test
    public void getVClockFromBytesField()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();

        final Object o = new Object()
        {
            @SuppressWarnings("unused")
            @RiakVClock
            private final byte[] domainProperty = clockBytes;

        };

        assertArrayEquals(clockBytes, VClockUtil.getVClock(o).getBytes());
    }

    @Test
    public void getVClockFromBytesMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final Object o = new Object()
        {
            @RiakVClock
            public byte[] getVClock()
            {
                return clockBytes;
            }
            
            @RiakVClock
            public void setVClock(byte[] bytes)
            {
                
            }
            
        };
        
        assertArrayEquals(clockBytes, VClockUtil.getVClock(o).getBytes());
        
    }
    
    @Test
    public void noVClockFieldOrMethod()
    {
        final Object o = new Object()
        {
            @SuppressWarnings("unused")
            private final String domainProperty = "tomatoes";

        };

        assertNull(VClockUtil.getVClock(o));
    }

    
    
    @Test
    public void nullVClockField()
    {
        final PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();

        assertNull(VClockUtil.getVClock(pojo));
    }
    
    @Test
    public void nullVClockMethod()
    {
        final PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        assertNull(VClockUtil.getVClock(pojo));
    }

    @Test
    public void illegalVClockFieldType()
    {
        final Object o = new Object()
        {
            @RiakVClock
            private final String domainProperty = null;

        };

        try
        {
            VClock vclock = VClockUtil.getVClock(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }

    }

    @Test
    public void illegalVClockGetterType()
    {
        final Object o = new Object()
        {
            @RiakVClock
            public String getVClock()
            {
                return null;
            }
            
            @RiakVClock
            public void setVClock(VClock v)
            {}

        };
        
        try
        {
            VClock vclock = VClockUtil.getVClock(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    public void illegalVClockSetterType()
    {
        final Object o = new Object()
        {
            @RiakVClock
            public VClock getVClock()
            {
                return null;
            }
            
            @RiakVClock
            public void setVClock(String v)
            {}

        };
        
        try
        {
            VClock vclock = VClockUtil.getVClock(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void setVClockField()
    {

        Object o = new Object()
        {
            @SuppressWarnings("unused")
            @RiakVClock
            private final VClock domainProperty = null;

        };

        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);

        VClockUtil.setVClock(o, expected);

        assertEquals(VClockUtil.getVClock(o), expected);

        o = new Object()
        {
            @SuppressWarnings("unused")
            @RiakVClock
            private final byte[] domainProperty = null;

        };

        VClockUtil.setVClock(o, expected);

        assertArrayEquals(VClockUtil.getVClock(o).getBytes(), clockBytes);

    }
    
    @Test
    public void setVClockMethod()
    {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        Object o = new Object()
        {
            private byte[] vclock;
            
            @RiakVClock
            public byte[] getVClock()
            {
                return vclock;
            }
            
            @RiakVClock
            public void setVClock(byte[] bytes)
            {
                this.vclock = bytes;
            }
            
        };
        
        final VClock expected = new BasicVClock(clockBytes);
        VClockUtil.setVClock(o, expected);
        assertEquals(VClockUtil.getVClock(o), expected);
        
        o = new Object()
        {
            private VClock vclock;
            
            @RiakVClock
            public VClock getVClock()
            {
                return vclock;
            }
            
            @RiakVClock
            public void setVClock(VClock vclock)
            {
                this.vclock = vclock;
            }
            
        };
        
        VClockUtil.setVClock(o, expected);
        assertEquals(VClockUtil.getVClock(o), expected);
        
    }
    
    
}
