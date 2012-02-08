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
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 *
 */
public class StoreObjectTest {

    private static final String BUCKET = "bucket";
    private static final String KEY = "key";

    @Mock private RawClient rawClient;
    @Mock private Converter<String> converter;
    @Mock private RiakResponse response;
    @Mock private IRiakObject rob;
    @Mock private Mutation<String> mutation;

    private ConflictResolver<String> conflictResolver;
    private Retrier retrier;
    
    private StoreObject<String> store;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        retrier = new DefaultRetrier(1);
        conflictResolver = new DefaultResolver<String>();
        store = new StoreObject<String>(rawClient, BUCKET, KEY, retrier);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.operations.StoreObject#execute()}.
     */
    @Test public void execute() throws Exception {
        // argument captors
        ArgumentCaptor<StoreMeta> storeCaptor = ArgumentCaptor.forClass(StoreMeta.class);
        ArgumentCaptor<FetchMeta> fetchCaptor = ArgumentCaptor.forClass(FetchMeta.class);

        // stub values
        final String expectedFetchResult = "fetchResult";
        final String mutatedValue = "mutatedValue";

        final byte[] vclockBytes = CharsetUtils.utf8StringToBytes("I am a vclock");
        final VClock vclock = new BasicVClock(vclockBytes);

        // all the fetch mock stubbing
        when(rawClient.fetch(eq(BUCKET), eq(KEY), any(FetchMeta.class))).thenReturn(response);
        when(response.numberOfValues()).thenReturn(1);
        when(response.iterator()).thenReturn(new RiakResponseIterator(rob)).thenReturn(new RiakResponseIterator(rob));
        when(response.getVclock()).thenReturn(vclock);
        when(converter.toDomain(rob)).thenReturn(expectedFetchResult).thenReturn(mutatedValue);

        // now the store stubbing
        when(mutation.apply(expectedFetchResult)).thenReturn(mutatedValue);
        when(converter.fromDomain(mutatedValue, vclock)).thenReturn(rob);
        when(rawClient.store(any(IRiakObject.class), any(StoreMeta.class))).thenReturn(response);

        // invoke
        String actual = store.r(2).pr(1).w(3).dw(4).pw(5)
            .basicQuorum(true)
            .notFoundOK(true)
            .ifNoneMatch(true)
            .ifNotModified(true)
            .returnDeletedVClock(true)
            .returnBody(true)
            .withRetrier(retrier)
            .withConverter(converter)
            .withMutator(mutation)
            .withResolver(conflictResolver)
            .execute();

        assertEquals(mutatedValue, actual);

        // verify
        verify(rawClient, times(1)).fetch(eq(BUCKET), eq(KEY), fetchCaptor.capture());
        verify(converter, times(2)).toDomain(rob);
        verify(converter, times(1)).fromDomain(mutatedValue, vclock);
        verify(rawClient, times(1)).store(eq(rob), storeCaptor.capture());

        // check fetch meta is populated as expected
        FetchMeta fm = fetchCaptor.getValue();

        assertEquals(true, fm.getBasicQuorum());
        assertEquals(true, fm.getNotFoundOK());
        assertEquals(true, fm.getReturnDeletedVClock());
        assertEquals(2, fm.getR().getIntValue());
        assertEquals(1, fm.getPr().getIntValue());

        // check store meta is populates as expected
        StoreMeta sm = storeCaptor.getValue();

        assertEquals(3, sm.getW().getIntValue());
        assertEquals(4, sm.getDw().getIntValue());
        assertEquals(5, sm.getPw().getIntValue());
        assertEquals(true, sm.getIfNoneMatch());
        assertEquals(true, sm.getIfNotModified());
        assertEquals(true, sm.getReturnBody());

    }

    private static final class RiakResponseIterator implements Iterator<IRiakObject> {
        private int total = 1;
        private final IRiakObject rob;

        private RiakResponseIterator(IRiakObject rob) {
            this.rob = rob;
        }

        public void remove() {}

        public IRiakObject next() {
            total--;
            return rob;
        }

        public boolean hasNext() {
            return total > 0;
        }
    }
}
