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

import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.indexes.StringBinIndex;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author roach
 */
public class RiakIndexConverterTest
{
    private RiakIndexConverter<DomainObject> converter;
    
    @Before 
    public void setUp()
    {
        this.converter = new RiakIndexConverter<DomainObject>();
    }
    
    @Test
    public void setGetPopulatedIndexes()
    {
        final String[] languages = {"c","erlang","java"};
        final HashSet<String> langSet = new HashSet<String>(Arrays.asList(languages));
        final Long[] numbers = {1L,2L,7L};
        final HashSet<Long> luckySet = new HashSet<Long>(Arrays.asList(numbers));
        
        
        RiakIndexes rIndexes = new RiakIndexes();
        rIndexes.getIndex(new StringBinIndex.Name("favorite_languages")).add(langSet);
        rIndexes.getIndex(new LongIntIndex.Name("lucky_numbers")).add(luckySet);
        rIndexes.getIndex(new LongIntIndex.Name("other_numbers")).add(luckySet);
        
        DomainObject obj = new DomainObject();
        
        converter.populateIndexes(rIndexes, obj);
        
        assertNotNull("Expected bin index field to be populated", obj.favoriteLanguages);
        assertNotNull("Expected int index field to be populated", obj.otherNumbers);
        assertEquals(langSet.size(), obj.favoriteLanguages.size());
        assertEquals(luckySet.size(), obj.otherNumbers.size());
        assertEquals(1, obj.lucky_number); // should be first in set
        
        rIndexes = converter.getIndexes(obj);
        
        assertNotNull("Expected RiakIndexes BinIndexes to be populated", 
                      rIndexes.hasIndex(new StringBinIndex.Name("favorite_languages")));
        assertNotNull("Expected RiakIndexes IntIndexes to be populated", 
                      rIndexes.hasIndex(new StringBinIndex.Name("other_numbers")));
        assertNotNull("Expected Method RiakIndexes IntIndexes to be populated",
                      rIndexes.hasIndex(new LongIntIndex.Name("calculated_longs")));
        assertNotNull("Expected Method RiakIndexes IntIndexes to be populated",
                      rIndexes.hasIndex(new LongIntIndex.Name("calculated_integers")));
        assertNotNull("Expected Method RiakIndexes BinIndexes to be populated",
                      rIndexes.hasIndex(new StringBinIndex.Name("calculated_strings")));
        assertEquals(langSet.size(), rIndexes.getIndex(new StringBinIndex.Name("favorite_languages")).size());
        assertEquals(luckySet.size(), rIndexes.getIndex(new LongIntIndex.Name("other_numbers")).size());
        assertEquals(1, rIndexes.getIndex(new LongIntIndex.Name("lucky_numbers")).size());
        assertEquals(DomainObject.CALCULATIONS_COUNT, rIndexes.getIndex(new LongIntIndex.Name("calculated_longs")).size());
        assertEquals(DomainObject.CALCULATIONS_COUNT, rIndexes.getIndex(new LongIntIndex.Name("calculated_integers")).size());
        assertEquals(DomainObject.CALCULATIONS_COUNT, rIndexes.getIndex(new StringBinIndex.Name("calculated_strings")).size());
        
    }
    
    private static final class DomainObject
    {
        public static final int CALCULATIONS_COUNT = 5;
        
        @RiakIndex(name = "favorite_languages") 
        public Set<String> favoriteLanguages;
        // int field should end up with first entry of index set
        @RiakIndex(name = "lucky_numbers")
        public int lucky_number;
        @RiakIndex(name = "other_numbers")
        public Set<Integer> otherNumbers;

        @RiakIndex(name = "calculated_longs")
        public Set<Long> getCalculatedLongs() {
          final Set<Long> calculatedLongs = new HashSet<Long>();
          for (long i = 0; i < CALCULATIONS_COUNT; i++) {
                calculatedLongs.add(i);
          }
          return calculatedLongs;
        }

        @RiakIndex(name = "calculated_integers")
        public Set<Integer> getCalculatedIntegers() {
            final Set<Integer> calculatedIntegers = new HashSet<Integer>();
            for (int i = 0; i < CALCULATIONS_COUNT; i++) {
                calculatedIntegers.add(i);
            }
            return calculatedIntegers;
        }

        @RiakIndex(name = "calculated_strings") 
        public Set<String> getCalculatedStrings() {
            final Set<String> calculatedStrings = new HashSet<String>();
            for (int i = 0; i < CALCULATIONS_COUNT; i++) {
                calculatedStrings.add(String.valueOf(i));
            }
            return calculatedStrings;
        }
        
    }
    
    
}
