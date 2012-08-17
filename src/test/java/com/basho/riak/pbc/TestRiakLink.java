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

import static com.basho.riak.test.util.ExpectedValues.BS_BUCKET;
import static com.basho.riak.test.util.ExpectedValues.BS_KEY;
import static com.basho.riak.test.util.ExpectedValues.BS_TAG;
import static com.basho.riak.test.util.ExpectedValues.BUCKET;
import static com.basho.riak.test.util.ExpectedValues.KEY;
import static com.basho.riak.test.util.ExpectedValues.TAG;
import static com.basho.riak.test.util.ExpectedValues.rpbLinks;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.basho.riak.protobuf.RiakKvPB.RpbLink;
import com.google.protobuf.ByteString;

/**
 * @author russell
 * 
 */
public class TestRiakLink {

    @Test public void fromRpbLink() {
        final RpbLink rpbLink = RpbLink.newBuilder().setBucket(BS_BUCKET).setKey(BS_KEY).setTag(BS_TAG).build();
        final RiakLink link = new RiakLink(rpbLink);

        assertBasicValues(link);
    }

    @Test public void fromByteStrings() {
        final RiakLink link = new RiakLink(BS_BUCKET, BS_KEY, BS_TAG);

        assertBasicValues(link);
    }
    
    @Test public void fromStrings() {
        final RiakLink link = new RiakLink(BUCKET, KEY, TAG);
        
        assertBasicValues(link);
    }
    
    @Test
    public void buildsRpbLinkFromState() {
        final RiakLink link = new RiakLink(BS_BUCKET, BS_KEY, BS_TAG);
        final RpbLink rpbLink = link.build();
        
        assertEquals(BS_BUCKET, rpbLink.getBucket());
        assertEquals(BS_KEY, rpbLink.getKey());
        assertEquals(BS_TAG, rpbLink.getTag());
    }
    
    private static void assertBasicValues(final RiakLink riakLink) {
        assertEquals(BS_BUCKET, riakLink.getBucket());
        assertEquals(BS_KEY, riakLink.getKey());
        assertEquals(BS_TAG, riakLink.getTag());
    }

    @Test
    public void decode() {
        final int numLinks = 10;
        final List<RpbLink> rpbLinks = rpbLinks(numLinks);
        
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

}
