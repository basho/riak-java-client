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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.MessageBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandler;
import io.netty.channel.ChannelOutboundMessageHandler;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakMessageCodec extends CombinedChannelDuplexHandler 
    implements ChannelInboundByteHandler, ChannelOutboundMessageHandler<RiakMessage>
{
    private final AtomicInteger inFlight = new AtomicInteger();
    
    public RiakMessageCodec()
    {
        init(new Decoder(), new Encoder());
    }
    
    private Decoder decoder() {
        return (Decoder) stateHandler();
    }

    private Encoder encoder() {
        return (Encoder) operationHandler();
    }
    
    @Override
    public ByteBuf newInboundBuffer(ChannelHandlerContext chc) throws Exception
    {
        return decoder().newInboundBuffer(chc);
    }

    @Override
    public void discardInboundReadBytes(ChannelHandlerContext chc) throws Exception
    {
        decoder().discardInboundReadBytes(chc);
    }

    @Override
    public void freeInboundBuffer(ChannelHandlerContext chc) throws Exception
    {
        decoder().freeInboundBuffer(chc);
    }

    @Override
    public MessageBuf<RiakMessage> newOutboundBuffer(ChannelHandlerContext chc) throws Exception
    {
        return encoder().newOutboundBuffer(chc);
    }

    @Override
    public void freeOutboundBuffer(ChannelHandlerContext chc) throws Exception
    {
        encoder().freeOutboundBuffer(chc);
    }

    private final class Encoder extends MessageToMessageEncoder<RiakMessage>
    {
        @Override
        protected Object encode(ChannelHandlerContext chc, RiakMessage msg) throws Exception
        {
            int length = msg.getData().length + 1;
            ByteBuf header = Unpooled.buffer(5);
            header.writeInt(length);
            header.writeByte(msg.getCode());
            inFlight.incrementAndGet();
            return Unpooled.wrappedBuffer(header.array(), msg.getData());
        }
    }
    
    private final class Decoder extends ByteToMessageDecoder
    {
        private final Logger logger = LoggerFactory.getLogger(RiakMessageCodec.class);
        
        @Override
        protected Object decode(ChannelHandlerContext chc, ByteBuf inbndn) throws Exception
        {
            // Make sure we have 4 bytes
            if (inbndn.readableBytes() < 4)
            {
                return null;
            }

            inbndn.markReaderIndex();
            int length = inbndn.readInt();

            // See if we have the full frame.
            if (inbndn.readableBytes() < length)
            {
                inbndn.resetReaderIndex();
                return null;
            }
            
            byte code = inbndn.readByte();
            byte[] array = new byte[length - 1];
            inbndn.readBytes(array);
            inFlight.decrementAndGet();
            return new RiakMessage(code,array);
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelInactive(ctx);
            int missingResponses = inFlight.get();
            if (missingResponses > 0)
            {
                logger.debug("channel id:{} gone inactive with {} missing response(s)", 
                             ctx.channel().id(), missingResponses);
            }
        }
    }
}
