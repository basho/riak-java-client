/*
 * Copyright 2013 NBasho Technologies Inc.
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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakPBMessageDecoder extends ByteToMessageDecoder
{
    @Override
    protected Object decode(ChannelHandlerContext chc, ByteBuf in) throws Exception
    {
        
        // Make sure we have 4 bytes
        if (in.readableBytes() < 4)
        {
            return null;
        }
        
        in.markReaderIndex();
        int length = in.readInt();
        
        // See if we have the full frame.
        if (in.readableBytes() < length)
        {
            in.resetReaderIndex();
            return null;
        }
        
        int code = (int)in.readByte();
        byte[] array = new byte[length - 1];
        in.readBytes(array);
        return new RiakPBMessage(code,array);
        
    }
    
}
