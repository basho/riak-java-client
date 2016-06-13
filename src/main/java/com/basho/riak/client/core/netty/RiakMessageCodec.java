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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakMessageCodec extends ByteToMessageCodec<RiakMessage>
{
    @Override
    protected void encode(ChannelHandlerContext ctx, RiakMessage msg, ByteBuf out) throws Exception
    {
        int length = msg.getData().length + 1;
        out.writeInt(length);
        out.writeByte(msg.getCode());
        out.writeBytes(msg.getData());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        // Make sure we have 4 bytes
        if (in.readableBytes() >= 4)
        {
            in.markReaderIndex();
            int length = in.readInt();

            // See if we have the full frame.
            if (in.readableBytes() < length)
            {
                in.resetReaderIndex();
                return;
            }
            else
            {
                byte code = in.readByte();
                byte[] array = new byte[length - 1];
                in.readBytes(array);
                out.add(new RiakMessage(code,array));
            }

        }
    }

}
