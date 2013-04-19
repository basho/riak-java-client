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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.Arrays;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.powermock.reflect.Whitebox;






/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakPbMessageCodecTest
{
    final private int SIZE_DATA = 30;
    private byte[] data;
    final private int SIZE_LENGTH = 4;
    final private int SIZE_CODE = 1;
    private byte code = 1;
    private ByteBuf buffer;
    private ChannelHandlerContext mockContext;
    
    @Before
    public void setUp() throws Exception
    {
        code = 1;
        data = new byte[SIZE_DATA];
        Arrays.fill(data, (byte)1);
        
        RiakPbMessage pbMessage = new RiakPbMessage(code, data);
        RiakPbMessageCodec codec = new RiakPbMessageCodec();
        
        mockContext = mock(ChannelHandlerContext.class);
        
        MessageToMessageEncoder<RiakPbMessage> encoder = Whitebox.invokeMethod(codec, "encoder", new Object[0]);
        
        buffer = Whitebox.invokeMethod(encoder, "encode", mockContext, pbMessage);
    }
    
    
    @Test
    public void encode() throws Exception
    {
        assertTrue(buffer.isReadable());
        assertEquals(buffer.readableBytes(), SIZE_DATA + SIZE_LENGTH + SIZE_CODE);
        
        int encodedLength = buffer.readInt();
        byte encodedCode = buffer.readByte();
        byte[] encodedData = new byte[SIZE_DATA];
        
        buffer.readBytes(encodedData);
        
        assertEquals(encodedCode, code);
        assertEquals(encodedLength, SIZE_DATA + SIZE_CODE);
        assertArrayEquals(data, encodedData);
    }
    
    @Test
    public void decode() throws Exception
    {
        RiakPbMessageCodec codec = new RiakPbMessageCodec();
        ByteToMessageDecoder decoder = Whitebox.invokeMethod(codec, "decoder", new Object[0]);
        
        RiakPbMessage message = Whitebox.invokeMethod(decoder, "decode", mockContext, buffer);
        
        assertEquals(code, message.getCode());
        assertArrayEquals(data, message.getData());
        
    }
}
