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

import com.basho.riak.newapi.RiakClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakFactory;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.query.NamedErlangFunction;

/**
 * @author russell
 * 
 */
public class ITestHTTPClient extends ITestClientBasic {
    
    /* (non-Javadoc)
     * @see com.basho.riak.client.itest.ITestClient#getClient()
     */
    @Override protected RiakClient getClient() throws RiakException {
        return RiakFactory.httpClient();
    }
    
    @Test public void fetchBucket()  throws RiakException {
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

    /* (non-Javadoc)
     * @see com.basho.riak.client.itest.ITestClient#createBucket()
     */
    @Override public void createBucket() throws RiakException {
        super.createBucket();
        
        final NamedErlangFunction newChashkeyFun = new NamedErlangFunction("riak_core_util", "chash_bucketonly_keyfun");
        final NamedErlangFunction newLinkwalkFun = new NamedErlangFunction("riak_core_util", "chash_std_keyfun");
        
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.createBucket(bucketName).chashKeyFunction(newChashkeyFun).linkWalkFunction(newLinkwalkFun).execute();
        
        assertEquals(newChashkeyFun, b.getChashKeyFunction());
        assertEquals(newLinkwalkFun, b.getLinkWalkFunction());
    }
    
    

}
