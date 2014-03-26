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
package com.basho.riak.client.core.fixture;

import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
abstract class Acceptor 
{
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    enum AcceptorType { ACCEPT_THEN_READ, ACCEPT_THEN_CLOSE }
    
    protected AcceptorType type = AcceptorType.ACCEPT_THEN_READ;
    protected final ServerSocketChannel server;
    protected final ByteBuffer readBuffer;
    
    protected final byte pbCode;
    protected final RiakKvPB.RpbGetResp pbMessage;
    protected final byte pbErrorMsgCode;
    protected final RiakPB.RpbErrorResp pbErrorMsg;
    
    public Acceptor(ServerSocketChannel server)
    {
        this.server = server;
        this.readBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        
        RiakKvPB.RpbContent content = RiakKvPB.RpbContent.newBuilder()
                                      .setValue(ByteString.copyFromUtf8("This is a value!"))
                                      .setVtag(ByteString.copyFromUtf8("garbage"))
                                      .build();
        
        pbMessage = RiakKvPB.RpbGetResp.newBuilder().addContent(content).setVclock(ByteString.copyFromUtf8("garbage")).build();
        pbErrorMsg = RiakPB.RpbErrorResp.newBuilder().setErrcode(0).setErrmsg(ByteString.copyFromUtf8("Riak Error")).build();
        pbCode = (byte)10; 
        pbErrorMsgCode = (byte)0;
    }
    
    final AcceptorType getType()
    {
        return type;
    }
    
    final ServerSocketChannel getServerSocketChannel()
    {
        return server;
    }
    
    void handle(SelectionKey key) throws IOException
    {
        if (((ReadableByteChannel)key.channel()).read(readBuffer) == -1) 
        {
            throw new IOException("Read on closed key");
        }
        
        boolean closeAfterWrite;
        readBuffer.flip();
        if (readBuffer.remaining() > 4)
        {
            int length = readBuffer.getInt();
            if (readBuffer.remaining() == length)
            {
                readBuffer.clear();
                closeAfterWrite = writePb(key);
                if (closeAfterWrite)
                {
                    key.channel().close();
                    key.cancel();
                }
            }
            else
            {
                readBuffer.position(readBuffer.limit());
                readBuffer.limit(readBuffer.capacity());
            }
        }
        else
        {
            readBuffer.position(readBuffer.limit());
            readBuffer.limit(readBuffer.capacity());
        }
                
        
        
    }
    
    abstract Acceptor duplicate();
    abstract boolean writePb(SelectionKey key) throws IOException;
    
}
