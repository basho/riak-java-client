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
package com.basho.riak.client.convert;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;


import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import com.basho.riak.client.convert.KeyUtil;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.VClockUtil;
import com.basho.riak.client.convert.RiakVClock;

/**
 * @author russell
 * 
 */
public class ConversionUtilTest {

    @Test public void getKey() {
        final String expected = "aKey";
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakKey private final String domainProperty = expected;

        };

        assertEquals(expected, KeyUtil.getKey(o));
    }

    @Test public void getNonStringKey() {
        final Date expected = Calendar.getInstance().getTime();
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakKey private final Date domainProperty = expected;

        };

        assertEquals(expected.toString(), KeyUtil.getKey(o));
    }

    @Test public void noKeyField() {
        final Object o = new Object() {
            @SuppressWarnings("unused") private final String domainProperty = "tomatoes";

        };

        assertNull(KeyUtil.getKey(o));
    }

    @Test public void nullKeyField() {
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakKey private final Date domainProperty = null;

        };

        assertNull(KeyUtil.getKey(o));
    }
    
    @Test public void getVClock () {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final VClock domainProperty = expected;

        };

        assertEquals(expected, VClockUtil.getVClock(o));
    }

    @Test public void getVClockFromBytes () {
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final byte[] domainProperty = clockBytes;

        };

        assertArrayEquals(clockBytes, VClockUtil.getVClock(o).getBytes());
    }
    
    @Test public void noVClockField() {
        final Object o = new Object() {
            @SuppressWarnings("unused") private final String domainProperty = "tomatoes";

        };

        assertNull(VClockUtil.getVClock(o));
    }

    @Test public void nullVClockField() {
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final VClock domainProperty = null;

        };

        assertNull(VClockUtil.getVClock(o));
    }
    
    @Test public void illegalVClockFieldType() {
        final Object o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final String domainProperty = null;

        };
        
        try {
            VClockUtil.getVClock(o);
            fail("Excepted IllegalArgumentException to be thrown");
        } catch (RuntimeException e) {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        
    }
    
    @Test public void setVClock() {
        
        Object o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final VClock domainProperty= null;

        };
        
        final byte[] clockBytes = "a85hYGBgzGDKBVIcypz/fvo/2e2UwZTImMfKwHIy/SRfFgA=".getBytes();
        final VClock expected = new BasicVClock(clockBytes);
        
        VClockUtil.setVClock(o, expected);
        
        assertEquals(VClockUtil.getVClock(o), expected);
        
        
        o = new Object() {
            @SuppressWarnings("unused") @RiakVClock private final byte[] domainProperty= null;

        };
        
        VClockUtil.setVClock(o, expected);
        
        assertArrayEquals(VClockUtil.getVClock(o).getBytes(), clockBytes);
        
        
    }
    

}
