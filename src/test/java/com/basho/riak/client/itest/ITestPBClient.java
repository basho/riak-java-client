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
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

/**
 * @author russell
 * 
 */
public class ITestPBClient extends ITestClientBasic {

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.itest.ITestClient#getClient()
     */
    @Override protected IRiakClient getClient() throws RiakException {
        PBClientConfig conf = new PBClientConfig.Builder().withHost(Hosts.RIAK_HOST).withPort(Hosts.RIAK_PORT).build();

        PBClusterConfig clusterConf = new PBClusterConfig(200);
        clusterConf.addClient(conf);

        return RiakFactory.newClient(clusterConf);
    }

    @Override @Test public void bucketProperties() throws Exception {
        final String bucket = UUID.randomUUID().toString();

        client.createBucket(bucket).allowSiblings(true).nVal(2).execute();

        Bucket b2 = client.fetchBucket(bucket).execute();

        assertEquals(true, b2.getAllowSiblings());
        assertEquals(new Integer(2), b2.getNVal());
    }
}
