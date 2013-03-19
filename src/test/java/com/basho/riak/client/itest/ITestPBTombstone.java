/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.itest;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestPBTombstone extends ITestTombstone
{

    @Override protected IRiakClient getClient() throws RiakException {
        PBClientConfig conf = new PBClientConfig.Builder().withHost(Hosts.RIAK_HOST).withPort(Hosts.RIAK_PORT).build();

        PBClusterConfig clusterConf = new PBClusterConfig(200);
        clusterConf.addClient(conf);

        return RiakFactory.newClient(clusterConf);
    }
    
}
