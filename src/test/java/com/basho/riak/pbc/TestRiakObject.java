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

import static com.basho.riak.test.util.ExpectedValues.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.basho.riak.pbc.RPB.RpbContent;


/**
 * @author russell
 * 
 */
public class TestRiakObject {

    @Test public void constuctFromStrings() {
        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT);
        assertBasicValues(riakObject);
    }
    
    @Test public void constuctFromByteStrings() {
        final RiakObject riakObject = new RiakObject(BS_BUCKET , BS_KEY, BS_CONTENT);
        assertBasicValues(riakObject);
    }
    
    @Test public void constuctFromStringsAndBytes() {
        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT.getBytes());
        assertBasicValues(riakObject);
    }
    
    @Test public void constuctFromByteStringsWithVclock() {
        final RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, BS_CONTENT);
        assertBasicValues(riakObject, true);
       
    }
    
    
    @Test public void fromRpbContent() {
        final String[] userMetaKeys = {"MetaKey1", "MetaKey2"};
        final String[] userMetaValues = {"MetaValue1", "MetaValue2"};
        
        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();
        
        contentBuilder.setContentType(BS_JSON_CTYPE);
        contentBuilder.setCharset(BS_UTF8_CHARSET);
        contentBuilder.setValue(BS_CONTENT);
        contentBuilder.setContentEncoding(BS_GZIP_ENC);
        contentBuilder.setVtag(BS_VTAG);
        contentBuilder.addAllLinks(rpbLinks(10));
        contentBuilder.setLastMod(LAST_MOD);
        contentBuilder.setLastModUsecs(LAST_MOD_USEC);
        contentBuilder.addAllUsermeta(rpbPairs(userMetaKeys, userMetaValues));
        
        final RpbContent content = contentBuilder.build();
        
        final RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, content);
        
        assertBasicValues(riakObject, true);
        
        assertEquals(content, riakObject.buildContent());
    }
    
    
    @Test public void addLink() {
        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT);
        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();
        
        contentBuilder.addAllLinks(rpbLinks(2)).setValue(BS_CONTENT);
        
        final RpbContent content = contentBuilder.build();
        
        riakObject.addLink(concatToByteString(TAG, 0), concatToByteString(BUCKET, 0), concatToByteString(KEY, 0));
        riakObject.addLink(TAG + "_1", BUCKET + "_1", KEY + "_1");
        
        assertEquals(content, riakObject.buildContent());
    }
    
    @Test public void setContenType() {
        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT);
        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();
        
        contentBuilder.setValue(BS_CONTENT).setContentType(BS_JSON_CTYPE);
        
        riakObject.setContentType(JSON_CTYPE);
        
        assertEquals(contentBuilder.build(), riakObject.buildContent());
    }
    
    private static void assertBasicValues(final RiakObject riakObject) {
        assertBasicValues(riakObject, false);
    }
    
    private static void assertBasicValues(final RiakObject riakObject, boolean hasVClock) {
        assertEquals(BUCKET, riakObject.getBucket());
        assertEquals(BS_BUCKET, riakObject.getBucketBS());
        assertEquals(KEY, riakObject.getKey());
        assertEquals(BS_KEY, riakObject.getKeyBS());
        assertEquals(BS_CONTENT, riakObject.getValue());
        
        if(hasVClock) {
            assertEquals(BS_VCLOCK, riakObject.getVclock());
        } else {
            assertNull("Expected null vclock", riakObject.getVclock());
        }
    }

}
