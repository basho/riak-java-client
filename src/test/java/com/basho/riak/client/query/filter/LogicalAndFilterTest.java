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
package com.basho.riak.client.query.filter;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.basho.riak.client.query.filter.FloatToStringFilter;
import com.basho.riak.client.query.filter.IntToStringFilter;
import com.basho.riak.client.query.filter.KeyFilter;
import com.basho.riak.client.query.filter.LogicalAndFilter;
import com.basho.riak.client.query.filter.SetMemberFilter;
import com.basho.riak.client.query.filter.SimilarToFilter;

/**
 * @author russell
 * 
 */
public class LogicalAndFilterTest {

    /**
     * Test method for
     * {@link com.basho.riak.client.query.filter.LogicalAndFilter#asArray()}.
     */
    @Test public void testAsArray() {
        final KeyFilter[] filters = new KeyFilter[] { new FloatToStringFilter(), new IntToStringFilter(),
                                                     new SetMemberFilter("rita", "sue", "bob") };
        LogicalAndFilter laf = new LogicalAndFilter(filters);
        laf.add(new SimilarToFilter("hippo", 2));
        
        assertArrayEquals(new Object[] { "and", new FloatToStringFilter().asArray(), new IntToStringFilter().asArray(),
                                        new SetMemberFilter("rita", "sue", "bob").asArray(), new SimilarToFilter("hippo", 2).asArray() }, laf.asArray());
    }

}
