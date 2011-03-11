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
package com.basho.riak.pbc;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.basho.riak.pbc.RPB.RpbBucketProps;
import com.basho.riak.pbc.RPB.RpbBucketProps.Builder;
import com.basho.riak.pbc.RPB.RpbGetBucketResp;

/**
 * @author russell
 * 
 */
public class TestBucketProperties {

    private static final int N_VAL = 2;

    private BucketProperties bucketProperties;

    @Test public void allFieldsAvailableAndSet() {
        final Builder rpbBucketPropsBuilder = RpbBucketProps.newBuilder().setAllowMult(true).setNVal(N_VAL);
        final RpbGetBucketResp bucketResponse = RpbGetBucketResp.newBuilder().setProps(rpbBucketPropsBuilder).build();

        bucketProperties = new BucketProperties();
        bucketProperties.init(bucketResponse);

        assertTrue("allow mult should be true", bucketProperties.getAllowMult());
        assertEquals("nVal incorrect", Integer.valueOf(N_VAL), bucketProperties.getNValue());
    }

    @Test public void noResponseDefaultValues() {
        final RpbGetBucketResp bucketResponse = RpbGetBucketResp.getDefaultInstance();

        bucketProperties = new BucketProperties();
        bucketProperties.init(bucketResponse);

        assertNull("allow mult should be null", bucketProperties.getAllowMult());
        assertNull("nVal should be null", bucketProperties.getNValue());
    }

    @Test public void emptyResponseDefaultValues() {
        final RpbBucketProps rpbBucketProps = RpbBucketProps.getDefaultInstance();
        final RpbGetBucketResp bucketResponse = RpbGetBucketResp.newBuilder().setProps(rpbBucketProps).build();

        bucketProperties = new BucketProperties();
        bucketProperties.init(bucketResponse);

        assertNull("allow mult should be null", bucketProperties.getAllowMult());
        assertNull("nVal should be null", bucketProperties.getNValue());
    }

    @Test public void build() {
        bucketProperties = new BucketProperties();
        bucketProperties.allowMult(true);
        bucketProperties.nValue(N_VAL);

        final RpbBucketProps rbpBucketProps = bucketProperties.build();

        assertEquals("built allow multi incorrect", true, rbpBucketProps.getAllowMult());
        assertEquals("nVal incorrect", N_VAL, rbpBucketProps.getNVal());
    }

    @Test public void equalsHashCode() {
        final Builder rpbBucketPropsBuilder = RpbBucketProps.newBuilder().setAllowMult(true).setNVal(N_VAL);
        final RpbGetBucketResp bucketResponse = RpbGetBucketResp.newBuilder().setProps(rpbBucketPropsBuilder).build();

        bucketProperties = new BucketProperties();
        bucketProperties.init(bucketResponse);

        assertTrue(bucketProperties.equals(bucketProperties));
        assertFalse(bucketProperties.equals(null));
        assertFalse(bucketProperties.equals("not a bucket properties"));

        final BucketProperties equalProperties = new BucketProperties();
        equalProperties.init(bucketResponse);

        assertTrue(equalProperties.equals(bucketProperties));
        assertTrue(bucketProperties.equals(equalProperties));
        assertEquals(equalProperties.hashCode(), bucketProperties.hashCode());

        final BucketProperties nullProperties = new BucketProperties();
        nullProperties.init(RpbGetBucketResp.getDefaultInstance());

        assertFalse(bucketProperties.equals(nullProperties));
        assertFalse(nullProperties.equals(bucketProperties));
        assertFalse(nullProperties.hashCode() == bucketProperties.hashCode());

        final Builder builder = RpbBucketProps.newBuilder().setAllowMult(false).setNVal(1);
        final RpbGetBucketResp response = RpbGetBucketResp.newBuilder().setProps(builder).build();

        final BucketProperties differentProperties =  new BucketProperties();
        differentProperties.init(response);
        assertFalse(bucketProperties.equals(differentProperties));
        assertFalse(differentProperties.equals(bucketProperties));
        assertFalse(differentProperties.hashCode() == bucketProperties.hashCode());
    }

}
