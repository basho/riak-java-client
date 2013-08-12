/*
 * Copyright 2013 Basho Technologies Inc.
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
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakNodeFixtureTest extends FixtureTest
{
    
    
    @Test
    public void operationSuccess() throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode node = 
            new RiakNode.Builder(Protocol.PB)
                        .withRemoteAddress("127.0.0.1")
                        .withPort(Protocol.PB, startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                        .build();
        
        node.start();
        FetchOperation<RiakObject> operation = new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        boolean accepted = node.execute(operation);
        assertTrue(accepted);
        RiakObject response = operation.get();
        assertEquals(response.getValueAsString(), "This is a value!");
        assertTrue(!response.isNotFound());
    }
    
    @Test(expected=ExecutionException.class)
    public void operationFail() throws UnknownHostException, InterruptedException, ExecutionException
    {
        RiakNode node = 
            new RiakNode.Builder(Protocol.PB)
                        .withRemoteAddress("127.0.0.1")
                        .withPort(Protocol.PB, startingPort + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                        .build();
        node.start();
        FetchOperation<RiakObject> operation = new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
        
        boolean accepted = node.execute(operation);
        RiakObject response = operation.get();
    }
    
    @Test(expected=ExecutionException.class)
    public void operationTimesOut() throws IOException, InterruptedException, ExecutionException
    {
        NetworkTestFixture nonRunningFixture = new NetworkTestFixture(8000);
        RiakNode node = 
            new RiakNode.Builder(Protocol.PB)
                        .withRemoteAddress("127.0.0.1")
                        .withPort(Protocol.PB, 8000 + NetworkTestFixture.ACCEPT_THEN_CLOSE)
                        .withReadTimeout(5000)
                        .build();
        node.start();
        FetchOperation<RiakObject> operation = new FetchOperation<RiakObject>(ByteString.copyFromUtf8("test_bucket"), ByteString.copyFromUtf8("test_key2"))
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());

        
        boolean accepted = node.execute(operation);
        RiakObject response = operation.get();
    }
    
    @Test
    public void nodeChangesStateOnPoolState() throws UnknownHostException, IOException, InterruptedException
    {
        RiakNode node = 
            new RiakNode.Builder(Protocol.HTTP)
                        .withRemoteAddress("127.0.0.1")
                        .withMinConnections(Protocol.HTTP, 10)
                        .withPort(Protocol.HTTP, startingPort + NetworkTestFixture.PB_FULL_WRITE_STAY_OPEN)
                        .build();

        node.start();
        NodeStateListener mockListener = mock(NodeStateListener.class);
        node.addStateListener(mockListener);
        
        try
        {
            fixture.shutdown();
            Thread.sleep(2000);
            verify(mockListener).nodeStateChanged(node, RiakNode.State.HEALTH_CHECKING);
            assertEquals(RiakNode.State.HEALTH_CHECKING, node.getNodeState());
        }
        finally
        {
            fixture = new NetworkTestFixture(startingPort);
            new Thread(fixture).start();
        }
        
    }
    
}
