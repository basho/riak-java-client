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
package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakPBMessage;
import com.basho.riak.client.core.RiakResponseListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakPBMessageHandler extends ChannelInboundMessageHandlerAdapter<RiakPBMessage>
{

    private final RiakResponseListener listener;
    
    public RiakPBMessageHandler(RiakResponseListener listener)
    {
        this.listener = listener;
    }
    
    public void messageReceived(ChannelHandlerContext chc, RiakPBMessage msg) throws Exception
    {
        listener.onSuccess(chc.channel(), msg);
        chc.channel().pipeline().remove(this);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        listener.onException(ctx.channel(), cause);
        ctx.channel().pipeline().remove(this);
    }
    
}
