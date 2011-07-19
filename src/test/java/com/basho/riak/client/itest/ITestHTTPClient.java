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
        HTTPClientConfig config = new HTTPClientConfig.Builder().withUrl(Hosts.RIAK_URL).build();
        HTTPClusterConfig conf = new HTTPClusterConfig(50);
        conf.addNode(config);
        return RiakFactory.newClient(conf);
    }

    @Test public void fetchBucket() throws RiakException {
        super.fetchBucket();
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();

        assertEquals(new NamedErlangFunction("riak_core_util", "chash_std_keyfun"), b.getChashKeyFunction());
        assertEquals(new NamedErlangFunction("riak_kv_wm_link_walker", "mapreduce_linkfun"), b.getLinkWalkFunction());
    }

    @Test public void updateBucket() throws RiakException {
        final NamedErlangFunction newChashkeyFun = new NamedErlangFunction("riak_core_util", "chash_bucketonly_keyfun");
        final NamedErlangFunction newLinkwalkFun = new NamedErlangFunction("riak_core_util", "chash_std_keyfun");

        super.updateBucket();

        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();

        b = client.updateBucket(b).chashKeyFunction(newChashkeyFun).linkWalkFunction(newLinkwalkFun).execute();

        assertEquals(newChashkeyFun, b.getChashKeyFunction());
        assertEquals(newLinkwalkFun, b.getLinkWalkFunction());
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

        Bucket b = client.createBucket(bucketName)
            .chashKeyFunction(newChashkeyFun)
            .linkWalkFunction(newLinkwalkFun)
            .r(Quora.ALL)
            .w(2)
            .dw(Quora.QUORUM)
            .rw(1)
            .execute();

        assertEquals(newChashkeyFun, b.getChashKeyFunction());
        assertEquals(newLinkwalkFun, b.getLinkWalkFunction());
        // TODO add extra properties to underlying transports, and expose them
        // assertEquals(Quora.ALL, b.getR());
        // assertEquals(2, b.getW());
        // assertEquals(Quora.QUORUM, b.getDW());
        // assertEquals(1, b.getRW());
    }

}
