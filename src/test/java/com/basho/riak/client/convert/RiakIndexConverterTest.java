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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.query.indexes.RiakIndexes;

/**
 * 
 * @author roach
 */
public class RiakIndexConverterTest {
    private RiakIndexConverter<DomainObject> converter;

    @Before public void setUp() {
        this.converter = new RiakIndexConverter<DomainObject>();
    }

    @Test public void setGetPopulatedIndexes() {
        final String[] languages = { "c", "erlang", "java" };
        final HashSet<String> langSet = new HashSet<String>(Arrays.asList(languages));
        final Integer[] numbers = { 1, 2, 7 };
        final HashSet<Integer> luckySet = new HashSet<Integer>(Arrays.asList(numbers));

        RiakIndexes rIndexes = new RiakIndexes();
        rIndexes.addBinSet("favorite_languages", langSet);
        rIndexes.addIntSet("lucky_numbers", luckySet);
        rIndexes.addIntSet("other_numbers", luckySet);

        DomainObject obj = new DomainObject();

        converter.populateIndexes(rIndexes, obj);

        assertNotNull("Expected bin index field to be populated", obj.favoriteLanguages);
        assertNotNull("Expected int index field to be populated", obj.otherNumbers);
        assertEquals(langSet.size(), obj.favoriteLanguages.size());
        assertEquals(luckySet.size(), obj.otherNumbers.size());
        assertEquals(1, obj.lucky_number); // should be first in set

        rIndexes = converter.getIndexes(obj);

        assertNotNull("Expected RiakIndexes BinIndexes to be populated", rIndexes.getBinIndex("favorite_languages"));
        assertNotNull("Expected RiakIndexes IntIndexes to be populated", rIndexes.getBinIndex("other_numbers"));
        assertEquals(langSet.size(), rIndexes.getBinIndex("favorite_languages").size());
        assertEquals(luckySet.size(), rIndexes.getIntIndex("other_numbers").size());
        assertEquals(1, rIndexes.getIntIndex("lucky_numbers").size());

    }

    private static final class DomainObject {
        @RiakIndex(name = "favorite_languages") public Set<String> favoriteLanguages;
        // int field should end up with first entry of index set
        @RiakIndex(name = "lucky_numbers") public int lucky_number;
        @RiakIndex(name = "other_numbers") public Set<Integer> otherNumbers;
    }

}