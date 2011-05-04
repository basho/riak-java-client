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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import com.basho.riak.client.convert.KeyUtil;
import com.basho.riak.client.convert.RiakKey;

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

}
