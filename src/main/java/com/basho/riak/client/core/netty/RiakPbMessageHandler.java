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

import com.basho.riak.client.core.RiakPbMessage;
import com.basho.riak.client.core.RiakResponseListener;
import com.basho.riak.client.util.pb.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakPbMessageHandler extends ChannelInboundMessageHandlerAdapter<RiakPbMessage>
{

    private final RiakResponseListener listener;
    private boolean timedOut = false;
    
    public RiakPbMessageHandler(RiakResponseListener listener)
    {
        this.listener = listener;
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext chc, RiakPbMessage msg) throws Exception
    {
        if (msg.getCode() == RiakMessageCodes.MSG_ErrorResp)
        {
            RiakPB.RpbErrorResp error = RiakPB.RpbErrorResp.newBuilder()
                                                .setErrcode(msg.getCode())
                                                .setErrmsg(ByteString.copyFrom(msg.getData()))
                                                .build(); 
            listener.onException(chc.channel(), new RiakResponseException(RiakMessageCodes.MSG_ErrorResp, error.getErrmsg().toStringUtf8()));
        }
        else
        {
            listener.onSuccess(chc.channel(), msg);
        }
        chc.channel().pipeline().remove(this);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception 
    {
        if (cause instanceof ReadTimeoutException)
        {
            timedOut = true;
            listener.onException(ctx.channel(), cause);
        }
        else
        {
            if (!timedOut)
            {
                listener.onException(ctx.channel(), cause);
            }
            ctx.channel().pipeline().remove(this);
        }
    }
    
}
