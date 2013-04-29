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

import com.basho.riak.client.RiakObject;
import com.basho.riak.client.core.converters.GetRespConverter;
import com.basho.riak.client.core.fixture.NetworkTestFixture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
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
        List<RiakNode.Builder> list = new LinkedList<RiakNode.Builder>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder(Protocol.HTTP)
                                        .withMinConnections(Protocol.HTTP, 10)
                                        .withPort(Protocol.HTTP, i + NetworkTestFixture.HTTP_FULL_WRITE_STAY_OPEN);
            list.add(builder);
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        cluster.start();
        
        GetOperation operation = new GetOperation();
        cluster.execute(operation);
        
        try
        {
            RiakObject response = operation.get();
            assertEquals(response.getValueAsString(), null);
            assertTrue(response.notFound());
        }
        catch(InterruptedException e)
        {
            
        }
        
        cluster.stop();
        
    }
    
    @Test(expected=ExecutionException.class)
    public void operationFail() throws UnknownHostException, ExecutionException, InterruptedException
    {
        List<RiakNode.Builder> list = new LinkedList<RiakNode.Builder>();
        
        for (int i = 5000; i < 8000; i += 1000)
        {
            RiakNode.Builder builder = new RiakNode.Builder(Protocol.HTTP)
                                        .withMinConnections(Protocol.HTTP, 10)
                                        .withPort(Protocol.HTTP, i + NetworkTestFixture.ACCEPT_THEN_CLOSE);
            list.add(builder);
        }
        
        RiakCluster cluster = new RiakCluster.Builder(list).build();
        
        cluster.start();
        
        GetOperation operation = new GetOperation();
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
    
    static class GetOperation extends FutureOperation<RiakObject>
    {
        @Override
        protected RiakObject convert(RiakResponse rawResponse) throws ExecutionException
        {
            List<RiakObject> rol = rawResponse.convertResponse(new GetRespConverter("bucket", "key", false));
            return rol.get(0);
        }

        @Override
        protected Object createChannelMessage(Protocol p)
        {
            switch(p)
            {
                case HTTP:
                    HttpRequest message = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
                    message.headers().set(HttpHeaders.Names.HOST, "localhost");
                    return message;
                default:
                    throw new IllegalArgumentException("Protocol not supported: " + p);
            }
        }
    }
}
