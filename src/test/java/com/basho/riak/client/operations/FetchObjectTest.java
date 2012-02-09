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
package com.basho.riak.client.operations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 * 
 */
public class FetchObjectTest {

    private static final String BUCKET = "bucket";
    private static final String KEY = "key";

    @Mock private RawClient rawClient;
    @Mock private Converter<String> converter;
    @Mock private RiakResponse response;
    @Mock private IRiakObject rob;

    private ConflictResolver<String> conflictResolver;
    private Retrier retrier;
    private FetchObject<String> fetch;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        retrier = new DefaultRetrier(1);
        conflictResolver = new DefaultResolver<String>();
        fetch = new FetchObject<String>(rawClient, BUCKET, KEY, retrier);
    }

    @Test public void fetch() throws Exception {
        ArgumentCaptor<FetchMeta> metaCaptor = ArgumentCaptor.forClass(FetchMeta.class);
        final String expected = "A horse! A Horse! My kingdom for a horse!";
        final Date modifiedSince = new Date();
        final byte[] vclockBytes = CharsetUtils.utf8StringToBytes("I am a vclock");
        final VClock vclock = new BasicVClock(vclockBytes);

        when(rawClient.fetch(eq(BUCKET), eq(KEY), any(FetchMeta.class))).thenReturn(response);
        when(response.numberOfValues()).thenReturn(1);
        when(response.iterator()).thenReturn(new Iterator<IRiakObject>() {
            private int total = 1;

            public void remove() {}

            public IRiakObject next() {
                total--;
                return rob;
            }

            public boolean hasNext() {
                return total > 0;
            }
        });

        when(converter.toDomain(rob)).thenReturn(expected);

        String actual = fetch.r(3).pr(4)
            .returnDeletedVClock(true).basicQuorum(true).notFoundOK(true)
            .withConverter(converter).withResolver(conflictResolver)
            .modifiedSince(modifiedSince).ifModified(vclock)
            .execute();

        assertEquals(expected, actual);

        verify(rawClient, times(1)).fetch(eq(BUCKET), eq(KEY), metaCaptor.capture());
        verify(converter, times(1)).toDomain(rob);

        // check fetch meta is populated as expected
        FetchMeta captured = metaCaptor.getValue();

        assertEquals(true, captured.getBasicQuorum());
        assertEquals(true, captured.getNotFoundOK());
        assertEquals(true, captured.getReturnDeletedVClock());
        assertEquals(3, captured.getR().getIntValue());
        assertEquals(4, captured.getPr().getIntValue());
        assertEquals(vclock, captured.getIfModifiedVClock());
        assertEquals(modifiedSince, captured.getIfModifiedSince());
    }
}
