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

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.RiakResponseListener;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;



/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RiakMessage.class)
public class RiakResponseHandlerTest
{
    private RiakResponseHandler handler;
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
        handler = new RiakResponseHandler(mockListener);
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
        RiakMessage message = PowerMockito.mock(RiakMessage.class);
        doReturn((byte)10).when(message).getCode();
        handler.channelRead(mockContext, message);
        
        verify(mockListener).onSuccess(mockChannel, message);
    }
    
}
