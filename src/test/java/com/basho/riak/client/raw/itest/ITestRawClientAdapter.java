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
package com.basho.riak.client.raw.itest;

import static com.basho.riak.client.AllTests.emptyBucket;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.test.util.ContentTypesEqual;

/**
 * @author russell
 * 
 */
public abstract class ITestRawClientAdapter {

    private RawClient client;
    private String bucketName;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        client = getClient();
        bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    @After public void tearDown() throws IOException{
        emptyBucket(bucketName, client);
    }
    protected abstract RawClient getClient() throws IOException;

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.pbc.PBClientAdapter#head(java.lang.String, java.lang.String, com.basho.riak.client.raw.FetchMeta)}
     * .
     */
    @Test public void head() throws Exception {
        final String key = "k";
        final String value = "v";

        IRiakObject o = RiakObjectBuilder.newBuilder(bucketName, key).withValue(value).build();

        client.store(o);

        RiakResponse r = client.fetch(bucketName, key);
        assertNotNull(r);
        assertTrue(r.hasValue());
        assertFalse(r.hasSiblings());
        assertEquals(r.getRiakObjects()[0].getValueAsString(), value);

        RiakResponse headResponse = client.head(bucketName, key, null);

        assertNotNull(headResponse);
        assertTrue(headResponse.hasValue());
        assertFalse(headResponse.hasSiblings());
        assertEmptyString(headResponse.getRiakObjects()[0].getValueAsString());
    }

    /**
     * @param valueAsString
     */
    private void assertEmptyString(String valueAsString) {
        if (valueAsString != null && !("".equals(valueAsString))) {
            fail("expected an empty or null string but got " + valueAsString);
        }
    }

    /**
     * @param valueAsString
     */
    private void assertNotEmptyString(String valueAsString) {
        if (valueAsString == null || "".equals(valueAsString)) {
            fail("expected an empty or null string but got " + valueAsString);
        }
    }
    /**
     * Test method for
     * {@link com.basho.riak.client.raw.pbc.PBClientAdapter#head(java.lang.String, java.lang.String, com.basho.riak.client.raw.FetchMeta)}
     * .
     */
    @Test public void headWithSiblings() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = "v";
        final String value2 = "v2";

        IRiakObject o = RiakObjectBuilder.newBuilder(bucketName, key).withValue(value).build();
        IRiakObject o2 = RiakObjectBuilder.newBuilder(bucketName, key).withValue(value2).build();

        client.updateBucket(bucketName, new BucketPropertiesBuilder().allowSiblings(true).build());

        // Sleep for bucket props to propagate
        Thread.sleep(500);

        client.store(o);
        client.store(o2);

        RiakResponse r = client.fetch(bucketName, key);
        assertNotNull(r);
        assertTrue(r.hasValue());
        assertTrue(r.hasSiblings());
        assertEquals(2, r.getRiakObjects().length);

        RiakResponse headResponse = client.head(bucketName, key, null);

        assertNotNull(headResponse);
        assertTrue(headResponse.hasValue());
        assertTrue(headResponse.hasSiblings());

        for (IRiakObject iro : headResponse.getRiakObjects()) {
            assertNotEmptyString(iro.getValueAsString());
        }

        client.updateBucket(bucketName, new BucketPropertiesBuilder().allowSiblings(false).build());
    }

    @Test public void storeReturnHeadOnly() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = "v";

        IRiakObject o = RiakObjectBuilder.newBuilder(bucketName, key).withValue(value).build();

        RiakResponse r = client.store(o, StoreMeta.headOnly());

        assertNotNull(r);
        assertTrue(r.hasValue());
        assertFalse(r.hasSiblings());
        assertEmptyString(r.getRiakObjects()[0].getValueAsString());
    }

    @Test public void storeSplitsContentTypeAndCharset() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = "{\"foo\": \"bar bar bar\"}";

        IRiakObject o = RiakObjectBuilder.newBuilder(bucketName, key).withContentType(Constants.CTYPE_JSON_UTF8).withValue(value).build();

        client.store(o);
        RiakResponse r = client.fetch(bucketName, key);

        IRiakObject o2 = r.getRiakObjects()[0];

        ContentTypesEqual.equal(Constants.CTYPE_JSON_UTF8, o2.getContentType());
    }
}
