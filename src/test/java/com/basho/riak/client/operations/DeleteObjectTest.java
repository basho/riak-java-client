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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 * 
 */
public class DeleteObjectTest {

    private static final String BUCKET = "bucket";
    private static final String KEY = "key";

    @Mock private RawClient rawClient;
    @Mock private RiakResponse response;

    private Retrier retrier;
    private DeleteObject deleteObject;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        retrier = new DefaultRetrier(1);
        deleteObject = new DeleteObject(rawClient, BUCKET, KEY, retrier);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.operations.DeleteObject#execute()}.
     */
    @Test public void allParameters() throws Exception {
        final ArgumentCaptor<DeleteMeta> deleteCaptor = ArgumentCaptor.forClass(DeleteMeta.class);
        final VClock vclock = new BasicVClock(CharsetUtils.utf8StringToBytes("I am a vclock"));

        deleteObject.r(1).pr(2).w(3).dw(4).pw(5).rw(6).vclock(vclock).execute();

        verify(rawClient, times(1)).delete(eq(BUCKET), eq(KEY), deleteCaptor.capture());

        // verify captured delete meta
        DeleteMeta dm = deleteCaptor.getValue();
        assertEquals(new Integer(1), dm.getR());
        assertEquals(new Integer(2), dm.getPr());
        assertEquals(new Integer(3), dm.getW());
        assertEquals(new Integer(4), dm.getDw());
        assertEquals(new Integer(5), dm.getPw());
        assertEquals(new Integer(6), dm.getRw());
        assertEquals(vclock, dm.getVclock());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.operations.DeleteObject#fetchBeforeDelete(boolean)}
     * .
     */
    @Test public void fetchBeforeDelete() throws Exception {
        final ArgumentCaptor<FetchMeta> fetchCaptor = ArgumentCaptor.forClass(FetchMeta.class);
        final ArgumentCaptor<DeleteMeta> deleteCaptor = ArgumentCaptor.forClass(DeleteMeta.class);
        final VClock vclock = new BasicVClock(CharsetUtils.utf8StringToBytes("I am a vclock"));

        when(rawClient.head(eq(BUCKET), eq(KEY), any(FetchMeta.class))).thenReturn(response);
        when(response.getVclock()).thenReturn(vclock);

        deleteObject.r(1).pr(2).w(3).dw(4).pw(5).rw(6).fetchBeforeDelete(true).execute();

        verify(rawClient, times(1)).head(eq(BUCKET), eq(KEY), fetchCaptor.capture());
        verify(rawClient, times(1)).delete(eq(BUCKET), eq(KEY), deleteCaptor.capture());

        // verify captured fetch meta
        FetchMeta fm = fetchCaptor.getValue();
        assertEquals(1, fm.getR().getIntValue());
        assertEquals(2, fm.getPr().getIntValue());

        // verify captured delete meta
        DeleteMeta dm = deleteCaptor.getValue();
        assertEquals(new Integer(1), dm.getR());
        assertEquals(new Integer(2), dm.getPr());
        assertEquals(new Integer(3), dm.getW());
        assertEquals(new Integer(4), dm.getDw());
        assertEquals(new Integer(5), dm.getPw());
        assertEquals(new Integer(6), dm.getRw());
        assertEquals(vclock, dm.getVclock());
    }
}
