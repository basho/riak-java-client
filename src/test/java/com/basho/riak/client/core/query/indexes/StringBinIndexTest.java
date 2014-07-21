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
package com.basho.riak.client.core.query.indexes;

import com.basho.riak.client.core.query.indexes.StringBinIndex;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * This only tests the conversions. RiakIndex is fully exercised via the RawIndexTest
 * @author Brian Roach <roach at basho dot com>
 */
public class StringBinIndexTest
{
    @Test
    public void storeAndRetrieveString()
    {
        StringBinIndex index = new StringBinIndex.Name("index_name").createIndex();
        Set<String> stringSet = new HashSet<String>(Arrays.asList("value1", "value2"));
        
        index.add(stringSet);
        
        assertEquals(index.size(), 2);
        assertTrue(index.hasValue("value1"));
        assertTrue(index.hasValue("value2"));
        
        Set<String> stringSet2 = index.values();
        assertTrue(stringSet.containsAll(stringSet2));
        
    }
}
