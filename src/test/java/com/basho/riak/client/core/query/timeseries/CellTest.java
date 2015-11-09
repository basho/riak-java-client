/*
 * Copyright 2015 Basho Technologies Inc
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

package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static junit.framework.Assert.*;

/**
 * Time Series Cell Unit Tests

 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class CellTest
{
    @Test
    public void TestStrings()
    {
        String s = "foobar";
        Cell c = new Cell(s);
        assertTrue(c.hasVarcharValue());
        assertEquals(c.getVarcharAsUTF8String(), s);
    }

    @Test
    public void TestBinaryValues()
    {
        BinaryValue bv = BinaryValue.createFromUtf8("foobar");
        Cell c = new Cell(bv);
        assertTrue(c.hasVarcharValue());
        assertEquals(c.getVarcharValue(), bv);
    }

    @Test
    public void TestLongs()
    {
        long l = 42;
        Cell c = new Cell(l);
        assertTrue(c.hasLong());
        assertEquals(c.getLong(), l);
    }

    @Test
    public void TestBigLongs()
    {
        long l = ((long)Integer.MAX_VALUE) + 1;
        Cell c = new Cell(l);
        assertTrue(c.hasLong());
        assertEquals(c.getLong(), l);
    }

    @Test
    public void TestDoubles()
    {
        double d = 42.0123456789123456789d;
        Cell c = new Cell(d);
        assertTrue(c.hasDouble());
        assertEquals(c.getDouble(), d);
    }
    @Test
    public void TestBooleans()
    {
        boolean b = true;
        Cell c = new Cell(b);
        assertTrue(c.hasBoolean());
        assertEquals(c.getBoolean(), b);
    }
    @Test
    public void TestCalendars()
    {
        Calendar ca = new GregorianCalendar();
        Cell c = new Cell(ca);
        assertTrue(c.hasTimestamp());
        assertEquals(c.getTimestamp(), ca.getTimeInMillis());
    }
    @Test
    public void TestDates()
    {
        Date d = new Date();
        Cell c = new Cell(d);
        assertTrue(c.hasTimestamp());
        assertEquals(c.getTimestamp(), d.getTime());
    }
    @Test
    public void TestRawTimestamps()
    {
        long t = new Date().getTime();
        Cell c = Cell.newTimestamp(t);
        assertTrue(c.hasTimestamp());
        assertEquals(c.getTimestamp(), t);
    }
}
