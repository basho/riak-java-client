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
package com.basho.riak.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;

/**
 * @author russell
 * 
 */
public class RiakFactoryTest {

    /**
     * Test method for
     * {@link com.basho.riak.client.RiakFactory#newClient(com.basho.riak.client.raw.config.Configuration)}
     * .
     * 
     * @throws RiakException
     */
    @Test public void validConfigsGetAClient() throws RiakException {
        HTTPClientConfig config = HTTPClientConfig.defaults();
        IRiakClient client = RiakFactory.newClient(config);

        assertNotNull(client);

        PBClientConfig pbConfig = PBClientConfig.defaults();
        IRiakClient pbBackedClient = RiakFactory.newClient(pbConfig);

        assertNotNull(pbBackedClient);

        try {
            RiakFactory.newClient(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // NO-OP
        }
    }

}
