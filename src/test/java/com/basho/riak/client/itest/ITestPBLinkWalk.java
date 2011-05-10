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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;

/**
 * @author russell
 * 
 */
public class ITestPBLinkWalk extends ITestLinkWalk {

    /* (non-Javadoc)
     * @see com.basho.riak.client.itest.ITestLinkWalk#getClient()
     */
    @Override protected IRiakClient getClient() throws RiakException {
        return RiakFactory.pbcClient();
    }

     
}
