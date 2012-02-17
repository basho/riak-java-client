/*
 * Copyright 2012 roach.
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
import com.basho.riak.client.query.NodeStats;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import java.util.Iterator;
import org.junit.Test;
import static org.junit.Assert.assertTrue;


/**
 *
 * @author roach
 */
public class ITestHTTPClusterStats extends ITestStats
{

    @Override 
    protected IRiakClient getClient() throws RiakException 
    {
        HTTPClientConfig config = new HTTPClientConfig.Builder()
                                    .withUrl(Hosts.RIAK_URL)
                                    .withMaxConnctions(50).build();
        
        HTTPClusterConfig conf = new HTTPClusterConfig(100);
        conf.addClient(config);
        conf.addClient(config);
        conf.addClient(config);
        
        return RiakFactory.newClient(conf);
    }
    
    @Test public void getStats() throws RiakException
    {
        Iterable<NodeStats> nodeStats = client.stats();
        Iterator<NodeStats> it = nodeStats.iterator();
        
        assertTrue(it.hasNext());
        
        int count = 0;
        for (NodeStats ns : nodeStats)
        {
            assertTrue(!ns.nodename().isEmpty());
            assertTrue(!ns.riakCoreVersion().isEmpty());
            count++;
        }
        
        // We added three clients to the Cluster Config
        assertTrue(count == 3);
        
    }
    
    
}
