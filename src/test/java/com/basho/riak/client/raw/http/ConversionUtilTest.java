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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.DeleteMeta.Builder;
import com.basho.riak.client.util.CharsetUtils;

/**
 * @author russell
 * 
 */
public class ConversionUtilTest {

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {}

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.ConversionUtil#convert(com.basho.riak.client.http.response.BucketResponse)}
     * .
     */
    @Test public void bucketResponseToBucketProperties() throws Exception {
        String bucketJSON = "{\"props\":{\"allow_mult\":true,\"basic_quorum\":true,\"big_vclock\":50,\"chash_keyfun\":{\"mod\":\"riak_core_util\",\"fun\":\"chash_std_keyfun\"},"
                            + "\"dw\":\"quorum\",\"last_write_wins\":false,\"linkfun\":{\"mod\":\"riak_kv_wm_link_walker\",\"fun\":\"mapreduce_linkfun\"},"
                            + "\"n_val\":3,\"name\":\"searchbucket\","
                            + "\"notfound_ok\":false,"
                            + "\"old_vclock\":86400,"
                            + "\"postcommit\":[],\"pr\":0,\"precommit\":[{\"mod\":\"riak_search_kv_hook\",\"fun\":\"precommit\"}, {\"name\": \"JS.fun\"}],"
                            + "\"pw\":0,\"r\":\"quorum\",\"rw\":\"quorum\","
                            + "\"search\":true,\"small_vclock\":10,\"w\":\"quorum\",\"young_vclock\":20}}";

        BucketResponse bucketResponse = mock(BucketResponse.class);
        when(bucketResponse.getBodyAsString()).thenReturn(bucketJSON);

        BucketProperties props = ConversionUtil.convert(bucketResponse);

        assertNotNull(props);
        assertEquals(true, props.getAllowSiblings());
        assertEquals(false, props.getLastWriteWins());
        assertNull(props.getBackend());
        assertEquals(50, props.getBigVClock().intValue());
        assertEquals(NamedErlangFunction.STD_CHASH_FUN, props.getChashKeyFunction());
        assertEquals(new Quorum(Quora.QUORUM), props.getDW());
        assertEquals(NamedErlangFunction.STD_LINK_FUN, props.getLinkWalkFunction());
        assertEquals(new Integer(3), props.getNVal());
        assertEquals(new Long(86400), props.getOldVClock());
        assertEquals(null, props.getPostcommitHooks());
        assertEquals(2, props.getPrecommitHooks().size());
        assertTrue(props.getPrecommitHooks().contains(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK));
        assertTrue(props.getPrecommitHooks().contains(new NamedJSFunction(("JS.fun"))));
        assertEquals(new Quorum(Quora.QUORUM), props.getR());
        assertEquals(new Quorum(Quora.QUORUM), props.getRW());
        assertEquals(new Long(20), props.getYoungVClock());
        assertEquals(new Integer(10), props.getSmallVClock());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.ConversionUtil#convert(com.basho.riak.client.bucket.BucketProperties)}
     * .
     * 
     * @throws IOException
     * @throws JSONException
     */
    @Test public void bucketPropertiesToBucketInfo() throws IOException, JSONException {
        BucketPropertiesBuilder builder = new BucketPropertiesBuilder();

        builder.allowSiblings(true);
        builder.nVal(2);
        builder.addPrecommitHook(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK);

        RiakBucketInfo bucketInfo = ConversionUtil.convert(builder.build());

        assertEquals(true, bucketInfo.getAllowMult());
        assertEquals(new Integer(2), bucketInfo.getNVal());

        JSONObject schema = bucketInfo.getSchema();

        JSONArray precommitHooks = (JSONArray) schema.get(Constants.FL_SCHEMA_PRECOMMIT);
        assertEquals(1, precommitHooks.length());
        JSONObject hook1 = (JSONObject) precommitHooks.get(0);

        assertEquals(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK.getMod(), hook1.get(Constants.FL_SCHEMA_FUN_MOD));
        assertEquals(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK.getFun(), hook1.get(Constants.FL_SCHEMA_FUN_FUN));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.ConversionUtil#convert(com.basho.riak.client.raw.DeleteMeta)}
     */
    @Test public void deleteMetaToRequestMeta() {
        DeleteMeta dm = null;

        RequestMeta rm = ConversionUtil.convert(dm);
        assertNull("Expected a null request meta for a null delete meta", rm);

        dm = new Builder().w(1).dw(2).pr(3).pw(4).r(5).rw(6).vclock(new BasicVClock(
                                                                                    CharsetUtils.utf8StringToBytes("vclock"))).build();

        rm = ConversionUtil.convert(dm);

        assertEquals("Expected w of 1", "1", rm.getQueryParam(Constants.QP_W));
        assertEquals("Expected dw of 2", "2", rm.getQueryParam(Constants.QP_DW));
        assertEquals("Expected pr of 3", "3", rm.getQueryParam(Constants.QP_PR));
        assertEquals("Expected pw of 4", "4", rm.getQueryParam(Constants.QP_PW));
        assertEquals("Expected r of 5", "5", rm.getQueryParam(Constants.QP_R));
        assertEquals("Expected rw of 6", "6", rm.getQueryParam(Constants.QP_RW));
        assertEquals("Expected vclock of 'vclock'", "vclock", rm.getHeader(Constants.HDR_VCLOCK));
    }
}
