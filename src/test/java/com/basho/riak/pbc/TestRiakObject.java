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

import java.util.Date;
import java.util.UUID;

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
    
    @Test public void addUserMetaDataItem() {
        final String[] userMetaKeys = { "MetaKey1", "MetaKey2" };
        final String[] userMetaValues = { "MetaValue1", "MetaValue2" };

        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT);
        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();

        contentBuilder.addAllUsermeta(rpbPairs(userMetaKeys, userMetaValues)).setValue(BS_CONTENT);

        final RpbContent content = contentBuilder.build();

        riakObject.addUsermetaItem(userMetaKeys[0], userMetaValues[0]);
        riakObject.addUsermetaItem(userMetaKeys[1], userMetaValues[1]);

        assertEquals(content, riakObject.buildContent());
    }

    @Test public void setContentType() {
        final RiakObject riakObject = new RiakObject(BUCKET, KEY, CONTENT);
        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();
        
        contentBuilder.setValue(BS_CONTENT).setContentType(BS_JSON_CTYPE);
        
        riakObject.setContentType(JSON_CTYPE);
        
        assertEquals(contentBuilder.build(), riakObject.buildContent());
    }

    @Test public void getLastModifiedDate() {
        final Date date = new Date();
        long time = date.getTime();
        long lastModified = time / 1000;
        long lastModifiedUsec = (time % 1000) * 100;

        final RpbContent.Builder contentBuilder = RpbContent.newBuilder();
        contentBuilder.setValue(BS_CONTENT).setLastMod((int)lastModified).setLastModUsecs((int)lastModifiedUsec);
        final RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, contentBuilder.build());

        assertEquals(date, riakObject.getLastModified());
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

    @Test public void modifyLinksAndBuildContentConcurrently() throws InterruptedException {
        final RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, BS_CONTENT);
        final int cnt = 20;

        Thread[] threads = new Thread[cnt];

        for (int i = 0; i < cnt; i++) {
            threads[i] = new Thread(new Runnable() {

                public void run() {
                    String bucket = UUID.randomUUID().toString();
                    String key = UUID.randomUUID().toString();
                    String tag = UUID.randomUUID().toString();
                    int cnt = 0;
                    while (true) {
                        riakObject.addLink(bucket + cnt, key + cnt, tag + cnt);
                        cnt++;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        Thread.sleep(500);

        riakObject.buildContent();
    }

    @Test public void modifyUserMetaAndBuildContentConcurrently() throws InterruptedException {
        final RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, BS_CONTENT);
        final int cnt = 20;

        Thread[] threads = new Thread[cnt];

        for (int i = 0; i < cnt; i++) {
            threads[i] = new Thread(new Runnable() {

                public void run() {
                    String key = UUID.randomUUID().toString();
                    String value = UUID.randomUUID().toString();
                    int cnt = 0;
                    while (true) {
                        riakObject.addUsermetaItem(key + cnt, value + cnt);
                        cnt++;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        Thread.sleep(500);

        riakObject.buildContent();
    }
}
