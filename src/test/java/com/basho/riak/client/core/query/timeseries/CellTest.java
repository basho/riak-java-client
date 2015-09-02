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

import static junit.framework.Assert.*;

import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.Constants;
import org.junit.Test;
import sun.nio.cs.US_ASCII;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class CellTest
{
    @Test
    public void TestStrings()
    {
        String s = "foobar";
        Cell c = new Cell(s);
        assertTrue(c.hasBinaryValue());
        assertEquals(c.getUtf8String(), s);
    }

    @Test
    public void TestBinaryValues()
    {
        BinaryValue bv = BinaryValue.createFromUtf8("foobar");
        Cell c = new Cell(bv);
        assertTrue(c.hasBinaryValue());
        assertEquals(c.getBinaryValue(), bv);
    }

    @Test
    public void TestInts()
    {
        int i = 42;
        Cell c = new Cell(i);
        assertTrue(c.hasInt());
        assertTrue(c.hasLong());
        assertEquals(c.getInt(), i);
    }

    @Test
    public void TestLongs()
    {
        long l = 42l;
        Cell c = new Cell(l);
        assertTrue(c.hasLong());
        assertTrue(c.hasInt());
        assertEquals(c.getLong(), l);
    }

    @Test
    public void TestBigLongs()
    {
        long l = ((long)Integer.MAX_VALUE) + 1;
        Cell c = new Cell(l);
        assertTrue(c.hasLong());
        assertFalse(c.hasInt());
        assertEquals(c.getLong(), l);
    }

    @Test
    public void TestFloats()
    {
        float f = 42.01f;
        Cell c = new Cell(f);
        assertFalse(c.hasNumeric());
        assertEquals(c.getFloat(), f);
    }
    @Test
    public void TestDoubles()
    {
        double d = 42.0123456789d;
        Cell c = new Cell(d);
        assertTrue(c.hasNumeric());
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
    public void TestRawNumeric()
    {
        byte[] ba = "-42.02".getBytes();
        Cell c = Cell.newRawNumeric(ba);
        assertTrue(c.hasNumeric());
        assertEquals(c.getRawNumeric(), ba);
    }
    @Test
    public void TestRawTimestamps()
    {
        long t = new Date().getTime();
        Cell c = Cell.newTimestamp(t);
        assertTrue(c.hasTimestamp());
        assertEquals(c.getTimestamp(), t);
    }

    @Test
    public void TestBCDEdgeCases()
    {
        Cell c = new Cell(-42.02f);

        String floatString = c.getRawNumericString();
        assertEquals('-', floatString.charAt(0));
        assertEquals('.', floatString.charAt(3));


        c = Cell.newRawNumeric("9.18E+09".getBytes(Charset.forName("US-ASCII")));
        float f = c.getFloat();
        assertEquals(9180000000f, f);

    }
}
