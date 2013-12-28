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

package com.basho.riak.client;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.operations.FetchOption;
import com.basho.riak.client.operations.FetchValue;
import com.basho.riak.client.operations.Key;
import com.basho.riak.client.operations.Location;
import com.basho.riak.client.operations.RiakClient;
import com.basho.riak.client.operations.StoreValue;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ApiDemo
{
    private final Logger logger = LoggerFactory.getLogger(ApiDemo.class);
    private final RiakClient client;
    
    public ApiDemo() throws UnknownHostException
    {
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);
        
        RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        
        client = new RiakClient(cluster);
        
    }
    
    public void fetch() throws ExecutionException, InterruptedException 
    {
        FetchValue<RiakObject> fetchCommand =
            new FetchValue<RiakObject>(Location.key("test_bucket", "key"))
                .withOption(FetchOption.R, Quorum.oneQuorum());
        
        
        FetchValue.Response<RiakObject> response = client.execute(fetchCommand);
        List<RiakObject> oList = response.getValue();
        for (RiakObject o : oList)
        {
            System.out.println(o.getValue().toStringUtf8());
        }
    }
    
    public void store() throws ExecutionException, InterruptedException 
    {
        RiakObject o = new RiakObject().setValue(ByteArrayWrapper.createFromUtf8("This is a value"));
                        
                        
        StoreValue<RiakObject> storeCommand =
            StoreValue.store(Location.key("test_bucket", "key"), o);
        
        StoreValue.Response<RiakObject> response = client.execute(storeCommand);
        
    }
    
    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException
    {
        ApiDemo ad = new ApiDemo();
        ad.store();
        ad.fetch();
        
    }
}
