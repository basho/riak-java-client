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

import com.basho.riak.client.core.Protocol;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

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
    protected final Protocol protocol;
    protected final ByteBuffer readBuffer;
    
    protected final byte pbCode;
    protected final RiakKvPB.RpbGetResp pbMessage;
    protected final String httpMessage;
    
    public Acceptor(ServerSocketChannel server, Protocol protocol)
    {
        this.server = server;
        this.protocol = protocol;
        this.readBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        
        httpMessage = "HTTP/1.1 404 Object Not Found\r\nServer: MochiWeb/1.1 WebMachine/1.9.2 (someone had painted it blue)\r\nDate: Tue, 02 Apr 2013 17:36:39 GMT\r\nContent-Type: text/plain\r\nContent-Length: 10\r\n\r\nnot found\n";
        
        RiakKvPB.RpbContent content = RiakKvPB.RpbContent.newBuilder()
                                      .setValue(ByteString.copyFromUtf8("This is a value!"))
                                      .build();
        
        pbMessage = RiakKvPB.RpbGetResp.newBuilder().addContent(content).build();
        pbCode = (byte)10; 
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
        switch(protocol)
        {
            case HTTP:
                readBuffer.flip();
                CharsetDecoder decoder = Charset.defaultCharset().newDecoder();
                CharBuffer charBuffer = decoder.decode(readBuffer);

                if (charBuffer.toString().endsWith("\r\n\r\n"))
                {
                    readBuffer.clear();
                    closeAfterWrite = writeHttp(key);
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
                break;
            case PB:
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
                break;
            default:
                throw new IllegalArgumentException("Invalid Protocol");
        }
        
        
    }
    
    abstract Acceptor duplicate();
    abstract boolean writeHttp(SelectionKey key) throws IOException;
    abstract boolean writePb(SelectionKey key) throws IOException;
    
}
