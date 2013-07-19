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
package com.basho.riak.client.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.Transport;

/**
 * @author russell
 * 
 */
public class WriteBucketTest {

    private static final String BUCKET = "b";

    @Mock private RawClient client;
    @Mock private Retrier retrier;

    private WriteBucket writeBucket;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        writeBucket = new WriteBucket(client, BUCKET, retrier);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.bucket.WriteBucket#execute()}.
     */
    @Test public void supportedTransport() {
        
        for (Transport t : new Transport[] { Transport.HTTP, Transport.PB } )
        {
            when(client.getTransport()).thenReturn(t);

            writeBucket.lastWriteWins(false);
            writeBucket.smallVClock(5);
            writeBucket.bigVClock(20);
            writeBucket.youngVClock(40);
            writeBucket.oldVClock(172800);
            writeBucket.r(1);
            writeBucket.w(1);
            writeBucket.dw(Quora.ONE);
            writeBucket.rw(Quora.QUORUM);
            writeBucket.pr(1);
            writeBucket.pw(1);
            writeBucket.notFoundOK(false);
            writeBucket.basicQuorum(true);
            writeBucket.backend("backend");
            writeBucket.enableForSearch();
        }
        
    }
}
