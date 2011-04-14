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
package com.basho.riak.client.raw.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.response.BucketResponse;

/**
 * @author russell
 * 
 */
public class TestKeySource {

    /**
     * This is a bit ropey (calling GC to cause ks to be unreachable) but it
     * needs testing
     * 
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "unused" }) @Test public void streamIsClosedWhenKeySourceIsWeaklyReachable() throws Exception {
        final BucketResponse bucketResponse = mock(BucketResponse.class);
        final RiakBucketInfo riakBucketInfo = mock(RiakBucketInfo.class);
        final Collection<String> keys = mock(Collection.class);
        final Iterator<String> iterator = mock(Iterator.class);

        when(bucketResponse.getBucketInfo()).thenReturn(riakBucketInfo);
        when(riakBucketInfo.getKeys()).thenReturn(keys);
        when(keys.iterator()).thenReturn(iterator);

        KeySource ks = new KeySource(bucketResponse);

        ks = null;

        System.gc();
        Thread.sleep(1000);

        verify(bucketResponse, times(1)).close();
    }

}
