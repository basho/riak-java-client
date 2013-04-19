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
package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakHttpMessage;
import com.basho.riak.client.core.RiakResponseListener;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import static org.mockito.Mockito.*;
import org.powermock.reflect.Whitebox;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakHttpMessageHandlerTest
{
    private RiakHttpMessageHandler handler;
    private ChannelHandlerContext mockContext;
    private Channel mockChannel;
    private ChannelPipeline mockPipeline;
    private RiakResponseListener mockListener;
    
    @Before
    public void setUp()
    {
        mockContext = mock(ChannelHandlerContext.class);
        mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockContext).channel();
        mockPipeline = mock(ChannelPipeline.class);
        doReturn(mockPipeline).when(mockChannel).pipeline();
        mockListener = mock(RiakResponseListener.class);
        handler = new RiakHttpMessageHandler(mockListener);
    }
    
    @Test
    public void registersListener()
    {
        RiakResponseListener listener = Whitebox.getInternalState(handler, "listener");
        assertEquals(listener, mockListener);
    }
    
    @Test
    public void notifiesListenerOnException() throws Exception
    {
        handler.exceptionCaught(mockContext, null);
        verify(mockListener).onException(mockChannel, null);
    }
    
    @Test
    public void notifiesListenerOnComplete() throws Exception
    {
        HttpResponse response = mock(HttpResponse.class);
        LastHttpContent lastContent = mock(LastHttpContent.class);
        
        handler.messageReceived(mockContext, response);
        handler.messageReceived(mockContext, lastContent);
        
        verify(mockListener).onSuccess(Matchers.any(Channel.class), Matchers.any(RiakHttpMessage.class));
    }
    
    @Test
    public void removesSelfFromPipelineOnException() throws Exception
    {
        handler.exceptionCaught(mockContext, null);
        verify(mockPipeline).remove(handler);
    }
    
    @Test
    public void removesSelfFromPipelineOnCompletion() throws Exception
    {
        HttpResponse response = mock(HttpResponse.class);
        LastHttpContent lastContent = mock(LastHttpContent.class);
        
        handler.messageReceived(mockContext, response);
        handler.messageReceived(mockContext, lastContent);
        
        verify(mockPipeline).remove(handler);
    }
    
    @Test
    public void producesCorrectResponse() throws Exception
    {
        HttpResponse response = mock(HttpResponse.class);
        HttpContent content = mock(HttpContent.class);
        LastHttpContent lastContent = mock(LastHttpContent.class);
        
        handler.messageReceived(mockContext, response);
        handler.messageReceived(mockContext, content);
        handler.messageReceived(mockContext, lastContent);
        
        RiakHttpMessage message = Whitebox.getInternalState(handler, "message");
        
        verify(mockListener).onSuccess(mockChannel, message);
        assertEquals(message.getResponse(), response);
        assertEquals(message.getContent().size(), 2);
        assertEquals(message.getContent().get(0), content);
        assertEquals(message.getContent().get(1), lastContent);
    }
}
