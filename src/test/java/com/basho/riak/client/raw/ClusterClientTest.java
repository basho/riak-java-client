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
package com.basho.riak.client.raw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;

/**
 * Tests that the abstract {@link ClusterClient} delegates and round robins
 * correctly
 * 
 * @author russell
 * 
 */
public class ClusterClientTest {

    private static final String BUCKET = "b";
    private static final String KEY = "k";
    private static final int QUORUM = 3;

    private static final RiakResponse RR = RiakResponse.empty();

    @Mock private RawClient client1;
    @Mock private RawClient client2;
    @Mock private RawClient client3;

    private RawClient[] cluster;

    private MockClusterClient client;

    @Before public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        cluster = new RawClient[] { client1, client2, client3 };
        client = new MockClusterClient(new PBClusterConfig(3));
    }

    @Test public void ping() throws IOException {
        client.ping();
        

        verify(client1, times(1)).ping();
        verify(client2, times(1)).ping();
        verify(client3, times(1)).ping();
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#fetch(java.lang.String, java.lang.String)}
     * .
     * 
     * @throws IOException
     */
    @Test public void fetchBK() throws IOException {
        for (RawClient rc : cluster) {
            when(rc.fetch(BUCKET, KEY)).thenReturn(RR);
        }

        for (int i = 0; i < 4; i++) {
            RiakResponse rr = client.fetch(BUCKET, KEY);
            assertEquals(RR, rr);
        }

        verify(client1, times(2)).fetch(BUCKET, KEY);
        verify(client2, times(1)).fetch(BUCKET, KEY);
        verify(client3, times(1)).fetch(BUCKET, KEY);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#fetch(java.lang.String, java.lang.String, int)}
     * .
     */
    @Test public void fetchBKQ() throws IOException {
        for (RawClient rc : cluster) {
            when(rc.fetch(BUCKET, KEY, QUORUM)).thenReturn(RR);
        }

        for (int i = 0; i < 6; i++) {
            RiakResponse rr = client.fetch(BUCKET, KEY, QUORUM);
            assertEquals(RR, rr);
        }

        verify(client1, times(2)).fetch(BUCKET, KEY, QUORUM);
        verify(client2, times(2)).fetch(BUCKET, KEY, QUORUM);
        verify(client3, times(2)).fetch(BUCKET, KEY, QUORUM);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#store(com.basho.riak.client.IRiakObject, com.basho.riak.client.raw.StoreMeta)}
     * .
     */
    @Test public void storeWithMeta() throws IOException {
        IRiakObject ro = RiakObjectBuilder.newBuilder(BUCKET, KEY).build();
        StoreMeta sm = new StoreMeta(QUORUM, QUORUM, QUORUM, false, false, false, false);

        for (RawClient rc : cluster) {
            when(rc.store(ro, sm)).thenReturn(RR);
        }

        for (int i = 0; i < 2; i++) {
            RiakResponse rr = client.store(ro, sm);
            assertEquals(RR, rr);
        }

        verify(client1, times(1)).store(ro, sm);
        verify(client2, times(1)).store(ro, sm);
        verify(client3, never()).store(ro, sm);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#store(com.basho.riak.client.IRiakObject)}
     * .
     */
    @Test public void store() throws IOException {
        IRiakObject ro = RiakObjectBuilder.newBuilder(BUCKET, KEY).build();

        for (int i = 0; i < 7; i++) {
            client.store(ro);
        }

        verify(client1, times(3)).store(ro);
        verify(client2, times(2)).store(ro);
        verify(client3, times(2)).store(ro);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#delete(java.lang.String, java.lang.String)}
     * .
     */
    @Test public void delete() throws IOException {
        for (int i = 0; i < 1; i++) {
            client.delete(BUCKET, KEY);
        }

        verify(client1, times(1)).delete(BUCKET, KEY);
        verify(client2, never()).delete(BUCKET, KEY);
        verify(client3, never()).delete(BUCKET, KEY);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#delete(java.lang.String, java.lang.String, int)}
     * .
     */
    @Test public void deleteWithQuroum() throws IOException {
        final String message = "thrown by client3";
        doThrow(new IOException(message)).when(client3).delete(BUCKET, KEY, QUORUM);

        boolean exceptionWasCaught = false;

        for (int i = 0; i < 3; i++) {
            try {
                client.delete(BUCKET, KEY, QUORUM);
            } catch (IOException e) {
                assertEquals(message, e.getMessage());
                exceptionWasCaught = true;
            }
        }

        assertTrue(exceptionWasCaught);

        verify(client1, times(1)).delete(BUCKET, KEY, QUORUM);
        verify(client2, times(1)).delete(BUCKET, KEY, QUORUM);
        verify(client3, times(1)).delete(BUCKET, KEY, QUORUM);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#listBuckets()}.
     */
    @Test public void listBuckets() throws IOException {
        final Set<String> expectedBuckets = new HashSet<String>();
        for (RawClient rc : cluster) {
            when(rc.listBuckets()).thenReturn(expectedBuckets);
        }

        for (int i = 0; i < 6; i++) {
            Set<String> buckets = client.listBuckets();
            assertEquals(expectedBuckets, buckets);
        }

        verify(client1, times(2)).listBuckets();
        verify(client2, times(2)).listBuckets();
        verify(client3, times(2)).listBuckets();
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#fetchBucket(java.lang.String)}
     * .
     */
    @Test public void fetchBucket() throws IOException {
        BucketProperties expectedProps = new BucketPropertiesBuilder().build();

        for (RawClient rc : cluster) {
            when(rc.fetchBucket(BUCKET)).thenReturn(expectedProps);
        }

        for (int i = 0; i < 3; i++) {
            BucketProperties props = client.fetchBucket(BUCKET);
            assertEquals(expectedProps, props);
        }

        verify(client1, times(1)).fetchBucket(BUCKET);
        verify(client2, times(1)).fetchBucket(BUCKET);
        verify(client3, times(1)).fetchBucket(BUCKET);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#updateBucket(java.lang.String, com.basho.riak.client.bucket.BucketProperties)}
     * .
     */
    @Test public void updateBucket() throws IOException {
        BucketProperties props = new BucketPropertiesBuilder().build();

        for (int i = 0; i < 3; i++) {
            client.updateBucket(BUCKET, props);
        }

        verify(client1, times(1)).updateBucket(BUCKET, props);
        verify(client2, times(1)).updateBucket(BUCKET, props);
        verify(client3, times(1)).updateBucket(BUCKET, props);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#listKeys(java.lang.String)}
     * .
     */
    @Test public void listKeys() throws IOException {
        StreamingOperation so = mock(StreamingOperation.class);
        Set<String> expectedKeys = new HashSet<String>(Arrays.asList("key1", "key2", "key3"));
        when(so.getAll()).thenReturn(expectedKeys);
        
        for (RawClient rc : cluster) {
            when(rc.listKeys(BUCKET)).thenReturn(so);
        }

        for (int i = 0; i < 1; i++) {
            Iterable<String> keys = client.listKeys(BUCKET).getAll();
            assertEquals(expectedKeys, keys);
        }

        verify(client1, times(1)).listKeys(BUCKET);
        verify(client2, never()).listKeys(BUCKET);
        verify(client3, never()).listKeys(BUCKET);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#linkWalk(com.basho.riak.client.raw.query.LinkWalkSpec)}
     * .
     */
    @Test public void testLinkWalk() throws IOException {
        LinkWalkSpec linkWalkSpec = mock(LinkWalkSpec.class);
        WalkResult expectedResult = mock(WalkResult.class);

        for (RawClient rc : cluster) {
            when(rc.linkWalk(linkWalkSpec)).thenReturn(expectedResult);
        }

        for (int i = 0; i < 9; i++) {
            WalkResult result = client.linkWalk(linkWalkSpec);
            assertEquals(expectedResult, result);
        }

        verify(client1, times(3)).linkWalk(linkWalkSpec);
        verify(client2, times(3)).linkWalk(linkWalkSpec);
        verify(client3, times(3)).linkWalk(linkWalkSpec);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#mapReduce(com.basho.riak.client.raw.query.MapReduceSpec)}
     * .
     * 
     * @throws MapReduceTimeoutException
     */
    @Test public void testMapReduce() throws IOException, MapReduceTimeoutException {
        MapReduceSpec mapReduceSpec = mock(MapReduceSpec.class);
        MapReduceResult expectedResult = mock(MapReduceResult.class);

        for (RawClient rc : cluster) {
            when(rc.mapReduce(mapReduceSpec)).thenReturn(expectedResult);
        }

        for (int i = 0; i < 9; i++) {
            MapReduceResult result = client.mapReduce(mapReduceSpec);
            assertEquals(expectedResult, result);
        }

        verify(client1, times(3)).mapReduce(mapReduceSpec);
        verify(client2, times(3)).mapReduce(mapReduceSpec);
        verify(client3, times(3)).mapReduce(mapReduceSpec);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#generateAndSetClientId()}.
     */
    @Test public void testGenerateAndSetClientId() throws IOException {
        byte[] clientId = new byte[] { 1, 2, 3, 4 };

        for (RawClient rc : cluster) {
            when(rc.generateAndSetClientId()).thenReturn(clientId);
        }

        for (int i = 0; i < 12; i++) {
            byte[] result = client.generateAndSetClientId();
            assertArrayEquals(clientId, result);
        }

        verify(client1, times(4)).generateAndSetClientId();
        verify(client2, times(4)).generateAndSetClientId();
        verify(client3, times(4)).generateAndSetClientId();
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#setClientId(byte[])}.
     */
    @Test public void testSetClientId() throws IOException {
        byte[] clientId = new byte[] { 1, 2, 3, 4 };

        for (int i = 0; i < 2; i++) {
            client.setClientId(clientId);
        }

        verify(client1, times(1)).setClientId(clientId);
        verify(client2, times(1)).setClientId(clientId);
        verify(client3, never()).setClientId(clientId);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.ClusterClient#getClientId()}.
     */
    @Test public void testGetClientId() throws IOException {
        byte[] clientId = new byte[] { 1, 2, 3, 4 };

        for (RawClient rc : cluster) {
            when(rc.getClientId()).thenReturn(clientId);
        }

        for (int i = 0; i < 12; i++) {
            byte[] result = client.getClientId();
            assertArrayEquals(clientId, result);
        }

        verify(client1, times(4)).getClientId();
        verify(client2, times(4)).getClientId();
        verify(client3, times(4)).getClientId();
    }

    /**
     * Test class implementation of {@link ClusterClient} that uses an array of
     * mock {@link RawClient} as the cluster
     * 
     * @author russell
     * 
     */
    private final class MockClusterClient extends ClusterClient<PBClientConfig> {

        /**
         * @param clusterConfig
         * @throws IOException
         */
        public MockClusterClient(ClusterConfig<PBClientConfig> clusterConfig) throws IOException {
            super(clusterConfig);
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * com.basho.riak.client.raw.ClusterClient#fromConfig(com.basho.riak
         * .client.raw.config.ClusterConfig)
         */
        @Override protected RawClient[] fromConfig(ClusterConfig<PBClientConfig> clusterConfig) throws IOException {
            return cluster;
        }

        /* (non-Javadoc)
         * @see com.basho.riak.client.raw.RawClient#getTransport()
         */
        public Transport getTransport() {
            return null;
        }
    }
}
