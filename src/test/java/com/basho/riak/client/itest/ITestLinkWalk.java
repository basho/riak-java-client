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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.RiakBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.WalkResult;

/**
 * @author russell
 * 
 */
public class ITestLinkWalk {

    @Test public void test_walk() throws RiakException {
        final IRiakClient client = RiakFactory.pbcClient();

        final String fooVal = "fooer";
        final String barVal = "barrer";
        
        final String bucketName = "test_walk_" + UUID.randomUUID().toString();
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
                assertEquals(fooVal, object.getValue());
            }

            assertEquals(1, s.size());
            stepsCnt++;
        }
        
        assertEquals(2, stepsCnt);

        assertTrue(keys.contains("second"));
        assertTrue(keys.contains("fourth"));
    }
}
