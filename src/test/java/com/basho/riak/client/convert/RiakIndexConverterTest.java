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

import com.basho.riak.client.query.indexes.RiakIndexes;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 1.4.5
 */
public class RiakIndexConverterTest extends ConversionUtilTest
{
    private HashSet<String> langs;
    private HashSet<Integer> integers;
    private HashSet<Long> longs;
    
    @Before
    public void setUp()
    {
        langs = new HashSet<String>(Arrays.asList("c","erlang","java"));
        integers = new HashSet<Integer>(Arrays.asList(1,2,7));
        longs = new HashSet<Long>(Arrays.asList(4L,9L,12L));
    }
    
    @Test
    public void testPopulateAndGetIndexFields()
    {
        final RiakIndexConverter<PojoWithAnnotatedFields<Void>> converter =
            new RiakIndexConverter<PojoWithAnnotatedFields<Void>>();
        RiakIndexes rIndexes = new RiakIndexes();
        
        rIndexes.addBinSet("favorite_languages", langs);
        rIndexes.addBinSet("lucky_language", langs);
        rIndexes.addIntSet("integers", integers);
        rIndexes.addIntSet("lucky_int", integers);
        rIndexes.addIntSet("lucky_integer", integers);
        rIndexes.addIntSet("longs", longs);
        rIndexes.addIntSet("lucky_long", longs);
        rIndexes.addIntSet("lucky_longlong", longs);
        
        PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();
        
        converter.populateIndexes(rIndexes, pojo);
        
        assertNotNull("Expected bin index field (Set<String> languages) to be populated", pojo.languages);
        assertNotNull("Expected bin index field (String luckyLanguage) to be populated", pojo.luckyLanguage);
        assertNotNull("Expected int index field (Set<Integer> integers) to be populated", pojo.integers);
        assertTrue("Expected int index field (int luckyInt) to be populated", pojo.luckyInt != 0);
        assertNotNull("Expected int index field (Integer luckyInteger) to be populated", pojo.luckyInteger);
        assertNotNull("Expected int index field (Set<Long> longs) to be populated", pojo.longs);
        assertTrue("Expected int index field (long luckyLong) to be populated", pojo.luckyLong != 0);
        assertNotNull("Expected int index field (Long luckyLongLong) to be populated", pojo.luckyLongLong);
        
        
        
        assertEquals(langs.size(), pojo.languages.size());
        assertEquals(langs.iterator().next(), pojo.luckyLanguage);
        assertEquals(integers.size(), pojo.integers.size());
        assertEquals(integers.iterator().next().intValue(), pojo.luckyInt);
        assertEquals(integers.iterator().next(), pojo.luckyInteger);
        assertEquals(longs.size(), pojo.longs.size());
        assertEquals(longs.iterator().next().longValue(), pojo.luckyLong);
        assertEquals(longs.iterator().next(), pojo.luckyLongLong);
        
        rIndexes = converter.getIndexes(pojo);
        
        assertEquals("Expected RiakIndexes BinIndex (favorite_languages) to be populated", 
                      rIndexes.getBinIndex("favorite_languages").size(), langs.size());        
        assertEquals("Expected RiakIndexes BinIndex (lucky_language) to be populated", 
                      rIndexes.getBinIndex("lucky_language").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (integers) to be populated", 
                      rIndexes.getIntIndex("integers").size(), integers.size());
        assertEquals("Expected RiakIndexes IntIndex (lucky_int) to be populated", 
                      rIndexes.getIntIndex("lucky_int").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (lucky_integer) to be populated", 
                      rIndexes.getIntIndex("lucky_integer").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getIntIndex("longs").size(), longs.size());
        assertEquals("Expected RiakIndexes IntIndex (lucky_long) to be populated", 
                      rIndexes.getIntIndex("lucky_long").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (lucky_longlong) to be populated", 
                      rIndexes.getIntIndex("lucky_longlong").size(), 1);
        
        assertTrue(rIndexes.getBinIndex("favorite_languages").containsAll(langs));
        assertEquals(rIndexes.getBinIndex("lucky_language").size(), 1);
        assertEquals(rIndexes.getBinIndex("lucky_language").iterator().next(), langs.iterator().next());
        
        // Legacy Integer support means conversion
        Set<Long> intIndex = rIndexes.getIntIndex("integers");
        for (Long l : intIndex)
        {
            assertTrue(integers.contains(l.intValue()));
        }
        assertEquals(rIndexes.getIntIndex("lucky_int").size(), 1);
        assertEquals(rIndexes.getIntIndex("lucky_int").iterator().next().intValue(), integers.iterator().next().intValue());
        assertEquals(rIndexes.getIntIndex("lucky_integer").size(), 1);
        assertEquals(rIndexes.getIntIndex("lucky_integer").iterator().next().intValue(), integers.iterator().next().intValue());
        
        assertTrue(rIndexes.getIntIndex("longs").containsAll(longs));
        assertEquals(rIndexes.getIntIndex("lucky_long").size(), 1);
        assertEquals(rIndexes.getIntIndex("lucky_long").iterator().next(), longs.iterator().next());
        assertEquals(rIndexes.getIntIndex("lucky_longlong").size(), 1);
        assertEquals(rIndexes.getIntIndex("lucky_longlong").iterator().next(), longs.iterator().next());
        
    }
    
    @Test
    public void testPopulateAndGetIndexMethods()
    {
        final RiakIndexConverter<PojoWithAnnotatedMethods<Void>> converter =
            new RiakIndexConverter<PojoWithAnnotatedMethods<Void>>();
        RiakIndexes rIndexes = new RiakIndexes();
        
        rIndexes.addBinSet("strings", langs);
        rIndexes.addIntSet("integers", integers);
        rIndexes.addIntSet("longs", longs);
        rIndexes.addIntSet("long", longs);
        rIndexes.addIntSet("longlong", longs);
        rIndexes.addIntSet("integer", integers);
        rIndexes.addIntSet("int", integers);
        rIndexes.addBinSet("string", langs);
        
        PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        
        converter.populateIndexes(rIndexes, pojo);
        
        assertNotNull("Expected bin index method (Set<String> getStrings) to be populated", pojo.getStrings());
        assertNotNull("Expected int index method (Set<Integer> getIntegers) to be populated", pojo.getIntegers());
        assertNotNull("Expected int index method (Set<Long> getLongs) to be populated", pojo.getLongs());
        assertNotNull("Expected bin index method (String getString) to be populated", pojo.getString());
        assertNotNull("Expected int index method (Integer getInteger) to be populated", pojo.getInteger());
        assertNotNull("Expected int index method (int getInt) to be populated", pojo.getInt());
        assertNotNull("Expected int index method (Long getLongLong) to be populated", pojo.getLongLong());
        assertNotNull("Expected int index method (Long getLong) to be populated", pojo.getLong());



        
        assertEquals(langs.size(), pojo.getStrings().size());
        assertEquals(integers.size(), pojo.getIntegers().size());
        assertEquals(longs.size(), pojo.getLongs().size());
        assertEquals(langs.iterator().next(), pojo.getString());
        assertEquals(integers.iterator().next(), pojo.getInteger());
        assertEquals(integers.iterator().next().intValue(), pojo.getInt());
        assertEquals(longs.iterator().next(), pojo.getLongLong());
        assertEquals(longs.iterator().next().longValue(), pojo.getLong());
        
        rIndexes = converter.getIndexes(pojo);
        
        assertEquals("Expected RiakIndexes BinIndex (strings) to be populated", 
                      rIndexes.getBinIndex("strings").size(), langs.size());
        assertEquals("Expected RiakIndexes IntIndex (integers) to be populated", 
                      rIndexes.getIntIndex("integers").size(), integers.size());
        assertEquals("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getIntIndex("longs").size(), longs.size());
        assertEquals("Expected RiakIndexes BinIndex (string) to be populated", 
                      rIndexes.getBinIndex("string").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (integer) to be populated", 
                      rIndexes.getIntIndex("integer").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (int) to be populated", 
                      rIndexes.getIntIndex("int").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (long) to be populated", 
                      rIndexes.getIntIndex("long").size(), 1);
        assertEquals("Expected RiakIndexes IntIndex (longlong) to be populated", 
                      rIndexes.getIntIndex("longlong").size(), 1);
        
        assertTrue(rIndexes.getBinIndex("strings").containsAll(langs));
        assertTrue(rIndexes.getIntIndex("longs").containsAll(longs));
        //Legacy Integer support means conversion
        Set<Long> intIndex = rIndexes.getIntIndex("integers");
        for (Long l : intIndex)
        {
            assertTrue(integers.contains(l.intValue()));
        }
        
        
    }
    
    @Test
    public void illegalRiakIndexFieldType()
    {
        final RiakIndexConverter<Object> converter = 
            new RiakIndexConverter<Object>();
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="whatever")
            private final Boolean domainProperty = null;

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="whatever")
            private final Set<Boolean> domainProperty = null;

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        
    }
    
    @Test
    public void illegalRiakIndexSetterType()
    {
        final RiakIndexConverter<Object> converter = 
            new RiakIndexConverter<Object>();
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Boolean b)
            {}
            
            @RiakIndex(name="whatever")
            public Set<Integer> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex(name="whatever")
            public void setIndex(Set<Boolean> index)
            {}
            
            @RiakIndex(name="whatever")
            public Set<Integer> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test
    public void missingIndexNameInAnnotation()
    {
        final RiakIndexConverter<Object> converter = 
            new RiakIndexConverter<Object>();
        final RiakIndexes rIndexes = new RiakIndexes();
        
        Object o = new Object()
        {
            @RiakIndex
            public void setIndex(Set<String> index)
            {}
            
            @RiakIndex
            public Set<String> getIndex() 
            {
                return null;
            }

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakIndex
            private String index = null;

        };
        
        try
        {
            converter.populateIndexes(rIndexes, o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    
}
