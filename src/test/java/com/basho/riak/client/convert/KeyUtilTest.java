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

import java.util.Calendar;
import java.util.Date;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class KeyUtilTest extends ConversionUtilTest
{

    @Test
    public void getKeyField()
    {
        final String expected = "aKey";
        final PojoWithAnnotatedFields<String> pojo = new PojoWithAnnotatedFields<String>();
        pojo.key = expected;

        assertEquals(expected, KeyUtil.getKey(pojo));
    }

    @Test
    public void getKeyMethod()
    {
        final String expected = "aKey";
        final PojoWithAnnotatedMethods<String> pojo = new PojoWithAnnotatedMethods<String>();
        pojo.setKey(expected);
        
        assertEquals(expected, KeyUtil.getKey(pojo));
    }
    
    @Test
    public void getNonStringKeyField()
    {
        final Date expected = Calendar.getInstance().getTime();
        final PojoWithAnnotatedFields<Date> pojo = new PojoWithAnnotatedFields<Date>();
        pojo.key = expected;
        
        assertEquals(expected.toString(), KeyUtil.getKey(pojo));
    }

    @Test
    public void getNonStringKeyMethod()
    {
        final Date expected = Calendar.getInstance().getTime();
        final PojoWithAnnotatedMethods<Date> pojo = new PojoWithAnnotatedMethods<Date>();
        pojo.setKey(expected);
        
        assertEquals(expected.toString(), KeyUtil.getKey(pojo));
    }
    
    @Test
    public void noKeyField()
    {
        final Object o = new Object()
        {
            private final String key = "tomatoes";
        };

        assertNull(KeyUtil.getKey(o));
    }

    @Test
    public void noKeyMethod()
    {
        final Object o = new Object();
        assertNull(KeyUtil.getKey(o));
    }
        
    
    @Test
    public void nullKeyField()
    {
        final String expected = null;
        final PojoWithAnnotatedFields<String> pojo = new PojoWithAnnotatedFields<String>();
        pojo.key = expected;

        assertNull(KeyUtil.getKey(pojo));
    }
    
    @Test
    public void nullKeyMethod()
    {
        final String expected = null;
        final PojoWithAnnotatedMethods<String> pojo = new PojoWithAnnotatedMethods<String>();
        pojo.setKey(expected);

        assertNull(KeyUtil.getKey(pojo));
    }
    
    
}
