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

import static org.junit.Assert.assertTrue;
import org.junit.Test;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakIndexesTest
{
    @Test
    public void addToIndex()
    {
        RiakIndexes indexes = new RiakIndexes();
        indexes.addToIndex(new LongIntIndex.Name("my_index"), 4L);
        
        assertTrue(indexes.hasIndex(new LongIntIndex.Name("my_index")));
        
        RiakIndex<Long> lii = indexes.getIndex(new LongIntIndex.Name("my_index"));
        assertTrue(lii.hasValue(4L));
    }
}
