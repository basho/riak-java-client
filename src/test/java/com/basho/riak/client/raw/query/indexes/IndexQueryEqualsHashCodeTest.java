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
package com.basho.riak.client.raw.query.indexes;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;

/**
 * @author russell
 * 
 */
public class IndexQueryEqualsHashCodeTest {

    private static final BinIndex BI1 = BinIndex.named("bi1");
    private static final BinIndex BI2 = BinIndex.named("bi2");
    private static final IntIndex II1 = IntIndex.named("ii1");
    private static final IntIndex II2 = IntIndex.named("ii2");

    private static final String BUCKET_1 = "B1";
    private static final String BUCKET_2 = "B2";

    private static final String BIN_RANGE_FROM = "a";
    private static final String BIN_RANGE_TO = "z";

    private static final int INT_RANGE_FROM = 0;
    private static final int INT_RANGE_TO = 100;

    @Test public void binRangeQueries() {
        // really, really, need property based testing t be exhaustive
        BinRangeQuery brq1 = new BinRangeQuery(BI1, BUCKET_1, BIN_RANGE_FROM, BIN_RANGE_TO);
        BinRangeQuery brq2 = new BinRangeQuery(BI1, BUCKET_1, BIN_RANGE_FROM, BIN_RANGE_TO);
        BinRangeQuery brq3 = new BinRangeQuery(BI2, BUCKET_1, BIN_RANGE_FROM, BIN_RANGE_TO);
        Object notABRQ = new Object();
        queryTest(brq1, brq2, brq3, notABRQ);
    }

    @Test public void binValueQueries() {
        // really, really, need property based testing t be exhaustive
        BinValueQuery bvq1 = new BinValueQuery(BI1, BUCKET_1, BIN_RANGE_FROM);
        BinValueQuery bvq2 = new BinValueQuery(BI1, BUCKET_1, BIN_RANGE_FROM);
        BinValueQuery bvq3 = new BinValueQuery(BI1, BUCKET_2, BIN_RANGE_FROM);
        Object notABVQ = new Object();
        queryTest(bvq1, bvq2, bvq3, notABVQ);
    }

    @Test public void intRangeQueries() {
        // really, really, need property based testing t be exhaustive
        IntRangeQuery irq1 = new IntRangeQuery(II1, BUCKET_1, INT_RANGE_FROM, INT_RANGE_TO);
        IntRangeQuery irq2 = new IntRangeQuery(II1, BUCKET_1, INT_RANGE_FROM, INT_RANGE_TO);
        IntRangeQuery irq3 = new IntRangeQuery(II1, BUCKET_1, INT_RANGE_TO, INT_RANGE_TO);
        Object notIRQ = new Object();
        queryTest(irq1, irq2, irq3, notIRQ);
    }

    @Test public void intValueQueries() {
        // really, really, need property based testing t be exhaustive
        IntValueQuery ivq1 = new IntValueQuery(II1, BUCKET_1, INT_RANGE_FROM);
        IntValueQuery ivq2 = new IntValueQuery(II1, BUCKET_1, INT_RANGE_FROM);
        IntValueQuery ivq3 = new IntValueQuery(II2, BUCKET_2, INT_RANGE_FROM);
        Object notIVQ = new Object();
        queryTest(ivq1, ivq2, ivq3, notIVQ);
    }

    private <T> void queryTest(T query, T query2, T different, Object wrongType) {
        assertTrue("Query should equal itself", query.equals(query));
        assertTrue("Queries with same properties should be equal", query.equals(query2));
        assertTrue("Equals should be reflexive", query2.equals(query));
        assertEquals("Queries with same properties should have equal hascodes", query.hashCode(), query2.hashCode());

        assertFalse("Queries with different properties should not be equal", query.equals(different));
        assertFalse("Queries with different properties should not be equal", different.equals(query));
        assertThat("Queries with different properties should not have equal hashcodes", query.hashCode(),
                   not(equalTo(different.hashCode())));
        assertFalse("Queries should not equal non-queries", query.equals(wrongType));
    }

}
