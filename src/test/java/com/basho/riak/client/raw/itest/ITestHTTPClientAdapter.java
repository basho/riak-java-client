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

import java.io.IOException;

import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPRiakClientFactory;

/**
 * @author russell
 * 
 */
public class ITestHTTPClientAdapter extends ITestRawClientAdapter {

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.itest.ITestRawClientAdapter#getClient()
     */
    @Override protected RawClient getClient() throws IOException {
        HTTPClientConfig config = new HTTPClientConfig.Builder().withUrl(Hosts.RIAK_URL).build();
        return HTTPRiakClientFactory.getInstance().newClient(config);
    }

}
