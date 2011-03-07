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
package com.basho.riak.pbc;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.basho.riak.pbc.RPB.RpbLink;
import com.google.protobuf.ByteString;

/**
 * @author russell
 * 
 */
public class TestRiakLink {

    private static final String BUCKET = "bucket";
    private static final String KEY = "key";
    private static final String TAG = "tag";
    
    private static final ByteString BS_BUCKET = ByteString.copyFromUtf8(BUCKET);
    private static final ByteString BS_KEY = ByteString.copyFromUtf8(KEY);
    private static final ByteString BS_TAG = ByteString.copyFromUtf8(TAG);

    @Test public void fromRpbLink() {
        final RPB.RpbLink rpbLink = RPB.RpbLink.newBuilder().setBucket(BS_BUCKET).setKey(BS_KEY).setTag(BS_TAG).build();
        final RiakLink link = new RiakLink(rpbLink);

        assertEquals(BS_BUCKET, link.getBucket());
        assertEquals(BS_KEY, link.getKey());
        assertEquals(BS_TAG, link.getTag());
    }

    @Test public void fromByteStrings() {
        final RiakLink link = new RiakLink(BS_BUCKET, BS_KEY, BS_TAG);

        assertEquals(BS_BUCKET, link.getBucket());
        assertEquals(BS_KEY, link.getKey());
        assertEquals(BS_TAG, link.getTag());
    }
    
    @Test public void fromStrings() {
        final RiakLink link = new RiakLink(BUCKET, KEY, TAG);
        
        assertEquals(BS_BUCKET, link.getBucket());
        assertEquals(BS_KEY, link.getKey());
        assertEquals(BS_TAG, link.getTag());
    }
    
    @Test
    public void buildsRpbLinkFromState() {
        final RiakLink link = new RiakLink(BS_BUCKET, BS_KEY, BS_TAG);
        final RpbLink rpbLink = link.build();
        
        assertEquals(BS_BUCKET, rpbLink.getBucket());
        assertEquals(BS_KEY, rpbLink.getKey());
        assertEquals(BS_TAG, rpbLink.getTag());
    }
    
    @Test
    public void decode() {
        final int numLinks = 10;
        final List<RpbLink> rpbLinks = new ArrayList<RpbLink>();
        
        for(int i=0; i < numLinks; i++) {
            RpbLink.Builder builder = RpbLink.newBuilder()
                .setBucket(concatToByteString(BUCKET, i))
                .setKey(concatToByteString(KEY, i))
                .setTag(concatToByteString(TAG, i));
            rpbLinks.add(builder.build());
        }
        
        final List<RiakLink> decoded = RiakLink.decode(rpbLinks);
        
        assertEquals(rpbLinks.size(), decoded.size());
        
        Set<Integer> counters = new HashSet<Integer>();
        
        for(RiakLink link : decoded) {
            String[] bucketNCnt = splitByteString(link.getBucket());
            String[] keyNCnt = splitByteString(link.getKey());
            String[] tagNCnt = splitByteString(link.getTag());
            
            assertEquals(BUCKET, bucketNCnt[0]);
            assertEquals(KEY, keyNCnt[0]);
            assertEquals(TAG, tagNCnt[0]);
            
            counters.add(Integer.parseInt(bucketNCnt[1]));
        }
        assertEquals(numLinks, counters.size());
    }

    private String[] splitByteString(final ByteString bs) {
        return bs.toStringUtf8().split("_");
    }
    
    private ByteString concatToByteString(String value, int counter) {
        return ByteString.copyFromUtf8(value + "_" + counter);
    }
    
}
