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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class TombstoneUtilTest extends ConversionUtilTest
{
    @Test
    public void getTombstoneField()
    {
        final PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();
        pojo.tombstone = true;
        assertTrue(TombstoneUtil.getTombstone(pojo));
    }
    
    @Test
    public void getTombstoneMethod()
    {
        final PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        pojo.setTombstone(true);
        assertTrue(TombstoneUtil.getTombstone(pojo));
    }
    
    @Test
    public void noTombstoneFieldOrMethod()
    {
        final Object o = new Object()
        {
            private final String domainProperty = "tomatoes";

        };

        assertFalse(TombstoneUtil.getTombstone(o));
    }
    
    @Test
    public void illegalTombstoneFieldType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            private final String domainProperty = null;

        };

        try
        {
            boolean tombstone = TombstoneUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void illegalTombstoneGetterType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            public String getTombstone()
            {
                return null;
            }
            
            @RiakVClock
            public void setTombstone(Boolean v)
            {}

        };
        
        try
        {
            boolean tombstone = TombstoneUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void illegalTombstoneSetterType()
    {
        final Object o = new Object()
        {
            @RiakTombstone
            public Boolean getTombstone()
            {
                return null;
            }
            
            @RiakVClock
            public void setTombstone(String v)
            {}

        };
        
        try
        {
            boolean tombstone = TombstoneUtil.getTombstone(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }
    
    @Test
    public void setTombstoneField()
    {
        Object o = new Object()
        {
            @RiakTombstone
            private final Boolean tombstone = null;

        };

        TombstoneUtil.setTombstone(o, true);
        assertEquals(TombstoneUtil.getTombstone(o), true);

        o = new Object()
        {
            @RiakTombstone
            private final boolean tombstone = false;

        };

        TombstoneUtil.setTombstone(o, true);
        assertEquals(TombstoneUtil.getTombstone(o), true);
    }
    
    @Test
    public void setTombstoneMethod()
    {
        Object o = new Object()
        {
            private Boolean tombstone;
            
            @RiakTombstone
            public Boolean getVClock()
            {
                return tombstone;
            }
            
            @RiakTombstone
            public void setVClock(Boolean tombstone)
            {
                this.tombstone = tombstone;
            }
            
        };
        
        TombstoneUtil.setTombstone(o, true);
        assertTrue(TombstoneUtil.getTombstone(o));
        
        o = new Object()
        {
            private boolean tombstone;
            
            @RiakTombstone
            public boolean getVClock()
            {
                return tombstone;
            }
            
            @RiakTombstone
            public void setVClock(boolean tombstone)
            {
                this.tombstone = tombstone;
            }
            
        };
        
        TombstoneUtil.setTombstone(o, true);
        assertTrue(TombstoneUtil.getTombstone(o));
        
    }
}
