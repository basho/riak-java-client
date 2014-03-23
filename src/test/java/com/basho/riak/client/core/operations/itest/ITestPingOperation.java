/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.PingOperation;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestPingOperation extends ITestBase
{
    @Test
    public void theMachinethatGoesPing() throws InterruptedException, ExecutionException
    {
        PingOperation ping = new PingOperation();
        cluster.execute(ping);
        PingOperation.Response resp = ping.get();
        assertTrue(resp.isSuccessful());
    }
    
    @Test
    public void theMachineThatDoesntGoPing() throws UnknownHostException, InterruptedException
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withRemotePort(10000)
                                        .withMinConnections(10);
        
        RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        
        PingOperation ping = new PingOperation();
        cluster.execute(ping);
        PingOperation.Response resp = ping.get();
        
        assertFalse(ping.isSuccess());
        assertNotNull(ping.cause());
        assertNotNull(ping.cause().getCause());
        
        
        cluster.shutdown();
        
    }
}
