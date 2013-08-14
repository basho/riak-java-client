/*
 * Copyright 2013 Basho Technologies Inc
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

import com.basho.riak.client.util.ByteArrayWrapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakIndexesTest
{
    private RiakIndexes indexes;
    
    @Before
    public void setup()
    {
        indexes = new RiakIndexes();
    }
    
    @Test
    public void createIndex()
    {
        assertTrue(indexes.isEmpty());
        LongIntIndex index = indexes.getIndex(new LongIntIndex.Name("foo"));
        assertFalse(indexes.isEmpty());
        assertEquals(indexes.size(), 1);
        assertTrue(indexes.hasIndex(new LongIntIndex.Name("foo")));
    }
    
    @Test
    public void addToIndex()
    {
        assertTrue(indexes.isEmpty());
        
        indexes.getIndex(new LongIntIndex.Name("my_index")).add(4L);
        
        assertTrue(indexes.hasIndex(new LongIntIndex.Name("my_index")));
        
        LongIntIndex lii = indexes.getIndex(new LongIntIndex.Name("my_index"));
        assertTrue(lii.hasValue(4L));
        
        RawIndex rri = indexes.getIndex(new RawIndex.Name("my_index", IndexType.INT));
        assertTrue(rri.hasValue(ByteArrayWrapper.unsafeCreate(String.valueOf(4L).getBytes())));
        
        assertEquals(indexes.size(), 1);
        
    }
    
    @Test
    public void removeIndex()
    {
        assertTrue(indexes.isEmpty());
        StringBinIndex stringIndex = indexes.getIndex(new StringBinIndex.Name("bar"));
        LongIntIndex longIndex = indexes.getIndex(new LongIntIndex.Name("foo"));
        
        LongIntIndex removedLongIndex = indexes.removeIndex(new LongIntIndex.Name("foo"));
        assertEquals(longIndex, removedLongIndex);
        assertEquals(indexes.size(), 1);
    }
    
    @Test
    public void getIndex()
    {
        indexes.getIndex(new LongIntIndex.Name("foo")).add(Long.MIN_VALUE);
        LongIntIndex index = indexes.getIndex(new LongIntIndex.Name("foo"));
        assertEquals(index.getName(), "foo");
        assertTrue(index.hasValue(Long.MIN_VALUE));
    }
    
    @Test
    public void size()
    {
        assertTrue(indexes.isEmpty());
        for (int i = 0; i < 5; i++)
        {
            indexes.getIndex(new StringBinIndex.Name("name" + i));
        }
        assertEquals(indexes.size(), 5);
        indexes.removeIndex(new StringBinIndex.Name("name0"));
        assertEquals(indexes.size(), 4);
        indexes.removeAllIndexes();
        assertEquals(indexes.size(), 0);
    }
    
    @Test
    public void empty()
    {
        assertTrue(indexes.isEmpty());
        for (int i = 0; i < 2; i++)
        {
            indexes.getIndex(new StringBinIndex.Name("name" + i));
        }
        assertFalse(indexes.isEmpty());
        indexes.removeIndex(new StringBinIndex.Name("name0"));
        assertFalse(indexes.isEmpty());
        indexes.removeIndex(new StringBinIndex.Name("name1"));
        assertTrue(indexes.isEmpty());
        indexes.getIndex(new LongIntIndex.Name("foo"));
        assertFalse(indexes.isEmpty());
        indexes.removeAllIndexes();
        assertTrue(indexes.isEmpty());
    }
    
    @Test
    public void indexTypesAreDifferent()
    {
        assertTrue(indexes.isEmpty());
        indexes.getIndex(new LongIntIndex.Name("foo")).add(Long.MIN_VALUE);
        indexes.getIndex(new StringBinIndex.Name("foo")).add("value");
        assertEquals(indexes.size(), 2);
        
        StringBinIndex stringIndex = indexes.getIndex(new StringBinIndex.Name("foo"));
        assertEquals(stringIndex.size(), 1);
        assertTrue(stringIndex.hasValue("value"));
        
        LongIntIndex longIndex = indexes.getIndex(new LongIntIndex.Name("foo"));
        assertEquals(stringIndex.size(), 1);
        assertTrue(longIndex.hasValue(Long.MIN_VALUE));
    }
    
}
