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

import static com.basho.riak.client.AllTests.emptyBucket;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.RiakBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 * 
 */
public abstract class ITestLinkWalk {

    protected IRiakClient client;
    protected String bucketName;

    @Before public void setUp() throws Exception {
        this.client = getClient();
        this.bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    @After public void teardown() throws Exception {
        emptyBucket(bucketName, client);
    }

    @Test public void test_walk() throws RiakException {
        final IRiakClient client = getClient();

        final String fooVal = "fooer";
        final String barVal = "barrer";

        final String[] first = { "first", "the first" };
        final String[] second = { "second", fooVal };
        final String[] third = { "third", barVal };
        final String[] fourth = { "fourth", fooVal };
        final String[] fith = { "fith", barVal };

        final String fooTag = "foo";
        final String barTag = "bar";

        final Bucket b = client.createBucket(bucketName).execute();
        final RiakBucket bucket = RiakBucket.newRiakBucket(b);

        IRiakObject o1 = RiakObjectBuilder.newBuilder(bucketName, first[0]).withValue(first[1]).addLink(bucketName,
                                                                                                       second[0],
                                                                                                       fooTag).addLink(bucketName,
                                                                                                                       third[0],
                                                                                                                       barTag).build();

        IRiakObject o2 = RiakObjectBuilder.newBuilder(bucketName, second[0]).withValue(second[1]).addLink(bucketName,
                                                                                                         fourth[0],
                                                                                                         fooTag).build();

        IRiakObject o3 = RiakObjectBuilder.newBuilder(bucketName, third[0]).withValue(third[1]).addLink(bucketName,
                                                                                                       fourth[0],
                                                                                                       fooTag).build();

        IRiakObject o4 = RiakObjectBuilder.newBuilder(bucketName, fourth[0]).withValue(fourth[1]).addLink(bucketName,
                                                                                                         fith[0],
                                                                                                         barTag).
                                                                                                         addUsermeta("metaKey", "123").build();

        IRiakObject o5 = RiakObjectBuilder.newBuilder(bucketName, fith[0]).withValue(fith[1]).build();

        bucket.store(o1);
        bucket.store(o2);
        bucket.store(o3);
        bucket.store(o4);
        bucket.store(o5);

        // Perform walk
        WalkResult result = client.walk(o1).addStep(bucketName, fooTag, true).addStep(bucketName, fooTag).execute();
        assertNotNull(result);

        int stepsCnt = 0;
        List<String> keys = new ArrayList<String>();
        for (Collection<IRiakObject> s : result) {

            for (IRiakObject object : s) {
                keys.add(object.getKey());
                assertEquals(fooVal, CharsetUtils.asString(object.getValue(), CharsetUtils.UTF_8));
            }

            assertEquals(1, s.size());
            stepsCnt++;
        }
        
        assertEquals(2, stepsCnt);

        assertTrue(keys.contains("second"));
        assertTrue(keys.contains("fourth"));
    }

    // Test the case in which the final nodes of the walk do not have any links themselves
    @Test public void test_walk_without_final_link() throws RiakException {
        final IRiakClient client = getClient();

        final String freundTag = "freund";

        final String[] first = { "benutzer_1234", "{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Brian Roache86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };
        final String[] second = { "benutzer_2345","{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Russell Browne86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };
        final String[] third = { "benutzer_3456","{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Sean Cribbse86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };

        final Bucket b = client.createBucket(bucketName).execute();
        final RiakBucket bucket = RiakBucket.newRiakBucket(b);

        IRiakObject o1 = RiakObjectBuilder.newBuilder(bucketName, first[0]).withValue(first[1]).addLink(bucketName,
                                                                                                        second[0],
                                                                                                        freundTag).build();

        IRiakObject o2 = RiakObjectBuilder.newBuilder(bucketName, second[0]).withValue(second[1]).addLink(bucketName,
                                                                                                          third[0],
                                                                                                          freundTag).build();

        IRiakObject o3 = RiakObjectBuilder.newBuilder(bucketName, third[0]).withValue(third[1]).build();

        bucket.store(o1);
        bucket.store(o2);
        bucket.store(o3);

        // Perform walk
        WalkResult result = client.walk(o1).addStep(bucketName, freundTag, true).addStep(bucketName, freundTag).execute();
        assertNotNull(result);

        int stepsCnt = 0;
        List<String> keys = new ArrayList<String>();
        for (Collection<IRiakObject> s : result) {

            for (IRiakObject object : s) {
                keys.add(object.getKey());
           }

            stepsCnt++;
        }

        assertEquals(2, stepsCnt);

        assertTrue(keys.contains("benutzer_2345"));
        assertTrue(keys.contains("benutzer_3456"));
    }

    /**
     * Tests the case where a the Link Walking tree has only a depth of one
     *
     */
    @Test public void test_walk_depth_of_one() throws RiakException {
        final IRiakClient client = getClient();

        final String friendTag = "friend";

        final String[] first = { "user_1234", "{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Brian Roache86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };
        final String[] second = { "user_2345","{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Russell Browne86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };
        final String[] third = { "user_3456","{e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;namee86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;:e86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;Sean Cribbse86cfcd75c1dbaa5bacb8e6d6415951a19054778quot;}" };

        final Bucket b = client.createBucket(bucketName).execute();
        final RiakBucket bucket = RiakBucket.newRiakBucket(b);

        IRiakObject o1 = RiakObjectBuilder.newBuilder(bucketName, first[0]).withValue(first[1]).addLink(bucketName,
                                                                                                       second[0],
                                                                                                       friendTag).addLink(bucketName,
                                                                                                                          third[0],
                                                                                                                          friendTag).build();

        IRiakObject o2 = RiakObjectBuilder.newBuilder(bucketName, second[0]).withValue(second[1]).build();

        IRiakObject o3 = RiakObjectBuilder.newBuilder(bucketName, third[0]).withValue(third[1]).build();

        bucket.store(o1);
        bucket.store(o2);
        bucket.store(o3);

        // Perform walk
        WalkResult result = client.walk(o1).addStep(bucketName, friendTag, true).addStep(bucketName, friendTag).execute();
        assertNotNull(result);

        int stepsCnt = 0;
        List<String> keys = new ArrayList<String>();
        for (Collection<IRiakObject> s : result) {

            for (IRiakObject object : s) {
                keys.add(object.getKey());
           }

            stepsCnt++;
        }

        assertEquals(2, stepsCnt);

        assertTrue(keys.contains("user_2345"));
        assertTrue(keys.contains("user_3456"));
    }

    /**
     * @return
     * @throws RiakException
     */
    protected abstract IRiakClient getClient() throws RiakException;
}
