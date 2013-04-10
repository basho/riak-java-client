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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakHttpMessageHandler extends ChannelInboundMessageHandlerAdapter<Object>
{
        private final RiakResponseListener listener;
    private RiakHttpMessage message;
    
    public RiakHttpMessageHandler(RiakResponseListener listener)
    {
        this.listener = listener;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        listener.onException(ctx.channel(), cause);
        ctx.channel().pipeline().remove(this);
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext chc, Object msg) throws Exception
    {
        if (msg instanceof HttpResponse)
        {
            message = new RiakHttpMessage((HttpResponse)msg);
        }
        
        if (msg instanceof HttpContent)
        {
            message.addContent((HttpContent)msg);
        
            if (msg instanceof LastHttpContent)
            {
                listener.onSuccess(chc.channel(), message);
                chc.channel().pipeline().remove(this);
            }
        }
    }
}
