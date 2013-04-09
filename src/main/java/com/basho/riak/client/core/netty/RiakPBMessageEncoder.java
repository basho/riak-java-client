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

import com.basho.riak.client.core.RiakPBMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
// TODO: Need to look at exception handling, this probably needs to be a stateful codec
// where it throws an exception on channel inactive if there's an outstanding message
public class RiakPBMessageEncoder extends MessageToMessageEncoder<RiakPBMessage>
{
    @Override
    protected Object encode(ChannelHandlerContext chc, RiakPBMessage i) throws Exception
    {
        int length = i.getData().length + 1;
        //ByteBuf header = PooledByteBufAllocator.DEFAULT.buffer(5);
        ByteBuf header = Unpooled.buffer(5);
        header.writeInt(length);
        header.writeByte(i.getCode());
        return Unpooled.wrappedBuffer(header.array(), i.getData());
    }
}
