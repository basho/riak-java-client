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
package com.basho.riak.client.itest;

import static com.basho.riak.client.Hosts.RIAK_URL;
import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.WalkResponse;

/**
 * Assumes Riak is reachable at {@link com.basho.riak.client.Hosts#RIAK_URL }.
 * @see com.basho.riak.client.Hosts#RIAK_URL
 */
public class ITestWalk {
    
    @Test
    public void test_walk() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = "test_walk";
        final String ROOT = "root";
        final String LEAF1 = "leaf1";
        final String LEAF2 = "leaf2";
        final String EXCLUDED_LEAF = "excluded_leaf";
        final byte[] INCLUDED_VALUE = "included".getBytes();
        final byte[] EXCLUDED_VALUE = "excluded".getBytes();
        final String TAG_INCLUDE = "tag_include";
        final String TAG_EXCLUDE = "tag_exclude";

        // Clear out the objects we're testing with
        assertSuccess(c.delete(BUCKET, ROOT));
        assertSuccess(c.delete(BUCKET, LEAF1));
        assertSuccess(c.delete(BUCKET, LEAF2));
        assertSuccess(c.delete(BUCKET, EXCLUDED_LEAF));
        
        // Add a few objects
        RiakObject leaf1 = new RiakObject(BUCKET, LEAF1, INCLUDED_VALUE);
        RiakObject leaf2 = new RiakObject(BUCKET, LEAF2, INCLUDED_VALUE);
        RiakObject excludedLeaf = new RiakObject(BUCKET, EXCLUDED_LEAF, EXCLUDED_VALUE);
        RiakObject root = new RiakObject(c, BUCKET, ROOT)
                            .addLink(new RiakLink(BUCKET, LEAF1, TAG_INCLUDE))
                            .addLink(new RiakLink(BUCKET, LEAF2, TAG_INCLUDE))
                            .addLink(new RiakLink(BUCKET, EXCLUDED_LEAF, TAG_EXCLUDE));
        assertSuccess(c.store(root, WRITE_3_REPLICAS()));
        assertSuccess(c.store(leaf1, WRITE_3_REPLICAS()));
        assertSuccess(c.store(leaf2, WRITE_3_REPLICAS()));
        assertSuccess(c.store(excludedLeaf, WRITE_3_REPLICAS()));
        
        // Perform walk
        WalkResponse walkresp = root.walk(BUCKET, TAG_INCLUDE).run();
        assertSuccess(walkresp);
        assertTrue(walkresp.hasSteps());
        assertEquals(1, walkresp.getSteps().size());
        assertEquals(2, walkresp.getSteps().get(0).size());
        
        // Verify expected only linked to objects are returned
        List<? extends List<RiakObject>> steps = walkresp.getSteps();
        List<String> keys = new ArrayList<String>();
        for (List<RiakObject> step : steps) {
            for (RiakObject object : step) {
                keys.add(object.getKey());
                assertArrayEquals(INCLUDED_VALUE, object.getValueAsBytes());
            }
        }
        assertTrue(keys.contains(LEAF1));
        assertTrue(keys.contains(LEAF2));
    }
}
