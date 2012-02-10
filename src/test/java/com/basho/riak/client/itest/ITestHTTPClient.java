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
package com.basho.riak.client.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;

/**
 * @author russell
 * 
 */
public class ITestHTTPClient extends ITestClientBasic {

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.itest.ITestClient#getClient()
     */
    @Override protected IRiakClient getClient() throws RiakException {
        HTTPClientConfig config = new HTTPClientConfig.Builder().withUrl(Hosts.RIAK_URL).withMaxConnctions(50).build();
        HTTPClusterConfig conf = new HTTPClusterConfig(100);
        conf.addClient(config);
        return RiakFactory.newClient(conf);
    }

    @Test public void fetchBucket() throws RiakException {
        super.fetchBucket();

        Bucket b = client.fetchBucket(bucketName).execute();

        assertEquals(new NamedErlangFunction("riak_core_util", "chash_std_keyfun"), b.getChashKeyFunction());
        assertEquals(new NamedErlangFunction("riak_kv_wm_link_walker", "mapreduce_linkfun"), b.getLinkWalkFunction());
    }

    @Test public void updateBucket() throws RiakException {
        final NamedErlangFunction newChashkeyFun = new NamedErlangFunction("riak_core_util", "chash_bucketonly_keyfun");
        final NamedErlangFunction newLinkwalkFun = new NamedErlangFunction("riak_core_util", "chash_std_keyfun");

        super.updateBucket();

        Bucket b = client.fetchBucket(bucketName).execute();

        b = client.updateBucket(b).chashKeyFunction(newChashkeyFun).linkWalkFunction(newLinkwalkFun).execute();

        assertEquals(newChashkeyFun, b.getChashKeyFunction());
        assertEquals(newLinkwalkFun, b.getLinkWalkFunction());
        client.updateBucket(b)
            .chashKeyFunction(new NamedErlangFunction("riak_core_util", "chash_std_keyfun"))
            .linkWalkFunction(new NamedErlangFunction("riak_kv_wm_link_walker", "mapreduce_linkfun"))
            .execute();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.itest.ITestClient#createBucket()
     */
    @Override public void createBucket() throws RiakException {
        super.createBucket();

        final NamedErlangFunction newChashkeyFun = new NamedErlangFunction("riak_core_util", "chash_bucketonly_keyfun");
        final NamedErlangFunction newLinkwalkFun = new NamedErlangFunction("riak_core_util", "chash_std_keyfun");

        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.createBucket(bucketName).chashKeyFunction(newChashkeyFun).linkWalkFunction(newLinkwalkFun).r(Quora.ALL).w(2).dw(Quora.QUORUM).rw(1).execute();

        assertEquals(newChashkeyFun, b.getChashKeyFunction());
        assertEquals(newLinkwalkFun, b.getLinkWalkFunction());
        client.updateBucket(b)
        .chashKeyFunction(new NamedErlangFunction("riak_core_util", "chash_std_keyfun"))
        .linkWalkFunction(new NamedErlangFunction("riak_kv_wm_link_walker", "mapreduce_linkfun"))
        .execute();
    }

    @Override @Test public void bucketProperties() throws Exception {
        final String bucket = UUID.randomUUID().toString();

        client.createBucket(bucket).allowSiblings(true).lastWriteWins(false)
            .smallVClock(5).bigVClock(20).youngVClock(40).oldVClock(172800)
            .nVal(2).r(1).w(1).dw(Quora.ONE).rw(Quora.QUORUM).pr(1).pw(1).notFoundOK(false).basicQuorum(true)
            .enableForSearch().execute();

        Bucket b2 = client.fetchBucket(bucket).execute();

        assertEquals(true, b2.getAllowSiblings());
        assertEquals(new Integer(2), b2.getNVal());

        assertEquals(false, b2.getLastWriteWins());
        assertEquals(new Integer(5), b2.getSmallVClock());
        assertEquals(new Integer(20), b2.getBigVClock());
        assertEquals(new Long(40), b2.getYoungVClock());
        assertEquals(new Long(172800), b2.getOldVClock());
        assertEquals(1, b2.getR().getIntValue());
        assertEquals(1, b2.getW().getIntValue());
        assertEquals(Quora.ONE, b2.getDW().getSymbolicValue());
        assertEquals(Quora.QUORUM, b2.getRW().getSymbolicValue());
        assertEquals(1, b2.getPR().getIntValue());
        assertEquals(1, b2.getPW().getIntValue());
        assertTrue(b2.getBasicQuorum());
        assertFalse(b2.getNotFoundOK());
        assertTrue(b2.getSearch());
        assertTrue(b2.isSearchEnabled());
        assertTrue(b2.getPrecommitHooks().contains(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK));

        //update the bucket again, disable search
        Bucket b3 = client.updateBucket(b2).disableSearch().execute();
        assertFalse(b3.isSearchEnabled());
        assertFalse(b3.getSearch());
        assertNull(b3.getPostcommitHooks());
    }
}
