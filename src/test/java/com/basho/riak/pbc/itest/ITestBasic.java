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
package com.basho.riak.pbc.itest;

import static com.basho.riak.client.http.Hosts.RIAK_HOST;
import static com.basho.riak.client.http.Hosts.RIAK_PORT;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;
import java.util.prefs.Preferences;

import org.junit.Test;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.pbc.BucketProperties;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

/**
 * Assumes Riak is reachable at {@link com.basho.riak.client.http.Hosts#RIAK_HOST }.
 * @author russell
 * @see com.basho.riak.client.http.Hosts#RIAK_HOST
 */
public class ITestBasic {

    private static final String BUCKET = "__itest_java_pbc__";

    /*
     * PING
     */

    @Test public void ping() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        c.ping();
    }

    /*
     * BUCKET PROPS
     */

    @Test public void bucketProperties() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        BucketProperties bucketInfo = new BucketProperties();
        bucketInfo.allowMult(true);
        bucketInfo.nValue(2);

        c.setBucketProperties(copyFromUtf8(BUCKET), bucketInfo);

        assertEquals(bucketInfo, c.getBucketProperties(copyFromUtf8(BUCKET)));

        //change them
        bucketInfo.allowMult(false);
        bucketInfo.nValue(3);

        c.setBucketProperties(copyFromUtf8(BUCKET), bucketInfo);

        assertEquals(bucketInfo, c.getBucketProperties(copyFromUtf8(BUCKET)));
    }

    /*
     * CLIENT ID
     */

    @Test public void prepareClientId() throws Exception {
        Preferences prefs = Preferences.userNodeForPackage(RiakClient.class);
        prefs.clear();

        String clid = prefs.get("client_id", null);
        assertNull(clid);

        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        c.prepareClientID();

        clid = prefs.get("client_id", null);
        assertNotNull(clid);
    }

    @Test public void clientId() throws Exception {
        final String clientId = "HUPA_PUPA";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        c.setClientID(clientId);
        assertEquals(clientId.substring(0, Constants.RIAK_CLIENT_ID_LENGTH), c.getClientID());
    }

    @Test public void clientIdExpiredConnection() throws Exception {
        final String clientId = "HUPA_PUPA";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        c.setClientID(clientId);

        Thread.sleep(1500);

        assertEquals(clientId.substring(0, Constants.RIAK_CLIENT_ID_LENGTH), c.getClientID());
    }

    @Test public void tooShortClientId() throws Exception {
        final String clientId = "HUP";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        try {
            c.setClientID(clientId);
            fail("Expected IllegalArgumentException");
        }catch(IllegalArgumentException e) {
            //NO-OP
        }
    }

    @Test public void tooLongClientId() throws Exception {
        final String clientId = "HUPA_PUPA";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        c.setClientID(copyFromUtf8(clientId));
        assertEquals(clientId.substring(0, Constants.RIAK_CLIENT_ID_LENGTH), c.getClientID());
    }

    /*
     * SERVER INFO
     */

    @Test public void getServerInfo() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        final Map<String, String> serverInfo = c.getServerInfo();

        assertTrue(serverInfo.containsKey("node"));
        assertTrue(serverInfo.containsKey("server_version"));
    }

    /*
     * LIST BUCKETS
     */
    @Test public void listBuckets() throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();
        final String content = "content";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        
        c.store(new RiakObject(bucket, key, content));

        final ByteString[] buckets = c.listBuckets();

        boolean testBucketPresent = false;

        for(ByteString b : buckets) {
            if(b.toStringUtf8().equals(bucket)) {
                testBucketPresent = true;
                break;
            }
        }

        assertTrue(testBucketPresent);

        c.delete(bucket, key);
    }

    /*
     * STORE RETRIEVE DELETE etc
     */

    @Test public void storeAndRetrieve() throws Exception {
        final String key = "key1";
        final String content = "value1";
        final String updatedContent = "updatedValue1";
        final String nonExistantKey = "non_existant_key";
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        c.prepareClientID();
        RiakObject o  = new RiakObject(BUCKET, key, content);

        //empty bucket
        for(ByteString k : c.listKeys(copyFromUtf8(BUCKET))) {
            c.delete(BUCKET, k.toStringUtf8());
        }

        c.store(o);
        RiakObject[] fetched = c.fetch(BUCKET, key);

        assertEquals(1, fetched.length);
        o = fetched[0];
        assertEquals(content, o.getValue().toStringUtf8());

        final RiakObject o2 = new RiakObject(o.getVclock(), o.getBucketBS(), o.getKeyBS(), copyFromUtf8(updatedContent));
        c.store(o2);

        fetched = c.fetch(BUCKET, key, 2);

        assertEquals(1, fetched.length);
        assertEquals(updatedContent, fetched[0].getValue().toStringUtf8());

        //fetch absent object
        c.delete(BUCKET, nonExistantKey, 2);
        fetched = c.fetch(BUCKET, nonExistantKey);

        assertTrue(fetched.length == 0);
    }
}
