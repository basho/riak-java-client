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
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.util.BinaryValue;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Note that the tests here exercise all the RiakIndex abstract methods
 * @author Brian Roach <roach at basho dot com>
 */
public class RawIndexTest
{
    
    
    @Test
    public void builderCreatesIndex()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.INT);
        RawIndex index = builder.createIndex();
        assertEquals(index.getName(), "index_name");
        assertEquals(index.getType(), IndexType.INT);
        assertEquals(index.getFullname(), "index_name" + IndexType.INT.suffix());
        
        builder = new RawIndex.Name("index_name", IndexType.BIN);
        index = builder.createIndex();
        assertEquals(index.getName(), "index_name");
        assertEquals(index.getType(), IndexType.BIN);
        assertEquals(index.getFullname(), "index_name" + IndexType.BIN.suffix());
        
    }
    
    @Test
    public void stripSuffix()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name_int", IndexType.INT);
        RawIndex index = builder.createIndex();
        assertEquals(index.getName(), "index_name");
    }
    
    @Test
    public void addValue()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        byte[] array = "value".getBytes();
        BinaryValue baw = BinaryValue.unsafeCreate(array);
        
        assertEquals(index.size(), 0);
        index.add(baw);
        assertEquals(index.size(), 1);
        assertTrue(index.hasValue(baw));
    }
    
    @Test
    public void addValues()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        assertEquals(index.size(), 5);
        
        for (BinaryValue baw : values)
        {
            assertTrue(index.hasValue(baw));
        }
    }
    
    @Test
    public void removeValue()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        BinaryValue baw = null;
        for (int i = 0; i < 3; i++)
        {
            baw = BinaryValue.unsafeCreate(("value" + i).getBytes());
            index.add(baw);
        }
        
        assertEquals(index.size(), 3);
        assertTrue(index.hasValue(baw));
        index.remove(baw);
        assertFalse(index.hasValue(baw));
        assertEquals(index.size(), 2);
    }
    
    @Test
    public void removeValues()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        assertEquals(index.size(), 5);
        BinaryValue baw = values.remove(0);
        index.remove(values);
        assertEquals(index.size(), 1);
        assertTrue(index.hasValue(baw));
        for (BinaryValue b : values )
        {
            assertFalse(index.hasValue(b));
        }
    }
    
    @Test
    public void noDuplicates()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        for (int i = 0; i < 5; i++)
        {
            index.add(BinaryValue.unsafeCreate("value".getBytes()));
        }
        
        assertEquals(index.size(), 1);
        
    }
    
    @Test
    public void wrap()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index2 = builder.wrap(index).createIndex();
        
        assertEquals(index, index2);
        assertNotSame(index, index2);
        assertTrue(index2.hasValue(values.get(0)));
        index.remove(values.get(0));
        assertFalse(index2.hasValue(values.get(0)));
        
    }
    
    @Test
    public void copy()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index2 = builder.copyFrom(index).createIndex();
        
        assertEquals(index, index2);
        assertNotSame(index, index2);
        assertTrue(index2.hasValue(values.get(0)));
        index.remove(values.get(0));
        assertTrue(index2.hasValue(values.get(0)));
    }
    
    @Test
    public void iterator()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        for (BinaryValue b : index)
        {
            assertTrue(index.hasValue(b));
        }
        
        Iterator<BinaryValue> i = index.iterator();
        
        while (i.hasNext())
        {
            BinaryValue b = i.next();
            i.remove();
            assertFalse(index.hasValue(b));
        }
    }
    
    @Test
    public void values()
    {
        RawIndex.Name builder = new RawIndex.Name("index_name", IndexType.BIN);
        RawIndex index = builder.createIndex();
        
        List<BinaryValue> values = new LinkedList<BinaryValue>();
        for (int i = 0; i < 5; i++)
        {
            values.add(BinaryValue.unsafeCreate(("value" + i).getBytes()));
        }
        
        index.add(values);
        
        Set<BinaryValue> valueSet = index.values();
        
        for (BinaryValue b : values)
        {
            assertTrue(valueSet.contains(b));
        }
    }
    
}
