/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.core;

import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.fixture.NetworkTestFixture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.RiakObject;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakClusterFixtureTest 
{
    private NetworkTestFixture[] fixtures;
    
    
    @Before
    public void setUp() throws IOException
    {
        fixtures = new NetworkTestFixture[3];
        for (int i = 0, j = 5000; i < 3; i++, j += 1000)
        {
            fixtures[i] = new NetworkTestFixture(j);
            new Thread(fixtures[i]).start();
        }
    }
    
    @After
    public void tearDown() throws IOException
    {
        for (int i = 0; i < fixtures.length; i++)
        {
            fixtures[i].shutdown();
        }
    }
    
    @Test
    public void operationSuccess() throws UnknownHostException, InterruptedException, ExecutionException
    {
        List<RiakNode> list = new LinkedList<RiakNode>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder(Protocol.PB)
                                        .withMinConnections(Protocol.PB, 10)
                                        .withPort(Protocol.PB, i + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN);
            list.add(builder.build());
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        cluster.start();
        
        FetchOperation<RiakObject> operation = new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());

        cluster.execute(operation);
        
        try
        {
            RiakObject response = operation.get();
            assertEquals(response.getValueAsString(), "This is a value!");
            assertTrue(!response.isNotFound());
        }
        catch(InterruptedException e)
        {
            
        }
        
        cluster.stop();
        
    }
    
    @Test(expected=ExecutionException.class)
    public void operationFail() throws UnknownHostException, ExecutionException, InterruptedException
    {
        List<RiakNode> list = new LinkedList<RiakNode>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder(Protocol.PB)
                                        .withMinConnections(Protocol.PB, 10)
                                        .withPort(Protocol.PB, i + NetworkTestFixture.ACCEPT_THEN_CLOSE);
            list.add(builder.build());
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        
        cluster.start();
        
        FetchOperation<RiakObject> operation = new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());

        cluster.execute(operation);
        
        try
        {
            RiakObject response = operation.get();
        }
        finally
        {
            cluster.stop();
        }
    }
    
    
}
