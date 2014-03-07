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
        
        assertNotNull("Expected RiakIndexes BinIndex (favorite_languages) to be populated", 
                      rIndexes.getBinIndex("favorite_languages"));        
        assertNotNull("Expected RiakIndexes BinIndex (lucky_language) to be populated", 
                      rIndexes.getBinIndex("lucky_language"));
        assertNotNull("Expected RiakIndexes IntIndex (integers) to be populated", 
                      rIndexes.getIntIndex("integers"));
        assertNotNull("Expected RiakIndexes IntIndex (lucky_int) to be populated", 
                      rIndexes.getIntIndex("lucky_int"));
        assertNotNull("Expected RiakIndexes IntIndex (lucky_integer) to be populated", 
                      rIndexes.getIntIndex("lucky_integer"));
        assertNotNull("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getIntIndex("longs"));
        assertNotNull("Expected RiakIndexes IntIndex (lucky_long) to be populated", 
                      rIndexes.getIntIndex("lucky_long"));
        assertNotNull("Expected RiakIndexes IntIndex (lucky_longlong) to be populated", 
                      rIndexes.getIntIndex("lucky_longlong"));
        
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
        
        PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        
        converter.populateIndexes(rIndexes, pojo);
        
        assertNotNull("Expected bin index field (Set<String> strings) to be populated", pojo.getStrings());
        assertNotNull("Expected int index field (Set<Integer> integers) to be populated", pojo.getIntegers());
        assertNotNull("Expected int index field (Set<Long> longs) to be populated", pojo.getLongs());
        assertEquals(langs.size(), pojo.getStrings().size());
        assertEquals(integers.size(), pojo.getIntegers().size());
        assertEquals(longs.size(), pojo.getLongs().size());

        rIndexes = converter.getIndexes(pojo);
        
        assertNotNull("Expected RiakIndexes BinIndex (strings) to be populated", 
                      rIndexes.getBinIndex("strings"));
        assertNotNull("Expected RiakIndexes IntIndex (integers) to be populated", 
                      rIndexes.getBinIndex("integers"));
        assertNotNull("Expected RiakIndexes IntIndex (longs) to be populated", 
                      rIndexes.getBinIndex("longs"));
        
        assertTrue(rIndexes.getBinIndex("strings").containsAll(langs));
        assertTrue(rIndexes.getIntIndex("longs").containsAll(longs));
        //Legacy Integer support means conversion
        Set<Long> intIndex = rIndexes.getIntIndex("integers");
        for (Long l : intIndex)
        {
            assertTrue(integers.contains(l.intValue()));
        }
        
        
    }
    
    
    

    
    
    
}
