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

import com.basho.riak.newapi.RiakClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakFactory;

/**
 * @author russell
 * 
 */
public class ITestDomainBucketPB extends ITestDomainBucket {

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.itest.ITestDomainBucket#getClient()
     */
    @Override public RiakClient getClient() throws RiakException {
        return RiakFactory.pbcClient();
    }

}
