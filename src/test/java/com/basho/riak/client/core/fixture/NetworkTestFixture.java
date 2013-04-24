/*
 * Copyright 2013 Basho Technologies Inc
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 *
 * This test fixture is to allow for specific network conditions to
 * be created and test that the client core reacts appropriately. 
 * 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class NetworkTestFixture implements Runnable
{
    public static int ACCEPT_THEN_CLOSE = 0;
    
    public static int PB_CLOSE_BEFORE_WRITE = 1;
    public static int PB_FULL_WRITE_THEN_CLOSE = 2;
    public static int PB_PARTIAL_WRITE_THEN_CLOSE = 3;
    public static int PB_FULL_WRITE_STAY_OPEN = 4;
    public static int PB_PARTIAL_WRITE_STAY_OPEN = 5;
    
    
    public static int HTTP_FULL_WRITE_THEN_CLOSE = 6;
    public static int HTTP_PARTIAL_WRITE_THEN_CLOSE = 7;
    public static int HTTP_FULL_WRITE_STAY_OPEN = 8;
    public static int HTTP_PARTIAL_WRITE_STAY_OPEN = 9;
    public static int HTTP_CLOSE_BEFORE_WRITE = 10;
    
    private Selector selector;
    
    public NetworkTestFixture() throws IOException
    {
        this(5000);
    }
    
    public NetworkTestFixture(int startingPort) throws IOException
    {
        selector = Selector.open();
        
        ServerSocketChannel server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + ACCEPT_THEN_CLOSE));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        SelectionKey key = server.keyFor(selector);
        key.attach(new AcceptThenClose(server, Protocol.HTTP));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + HTTP_CLOSE_BEFORE_WRITE ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadThenClose(server, Protocol.HTTP));
        
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + HTTP_FULL_WRITE_THEN_CLOSE ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadWriteThenClose(server, Protocol.HTTP));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + HTTP_PARTIAL_WRITE_THEN_CLOSE));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadPartialWriteThenClose(server, Protocol.HTTP));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + HTTP_FULL_WRITE_STAY_OPEN ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadWriteStayOpen(server, Protocol.HTTP));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + HTTP_PARTIAL_WRITE_STAY_OPEN));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadPartialWriteStayOpen(server, Protocol.HTTP));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + PB_CLOSE_BEFORE_WRITE ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadThenClose(server, Protocol.PB));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + PB_FULL_WRITE_THEN_CLOSE ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadWriteThenClose(server, Protocol.PB));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + PB_PARTIAL_WRITE_THEN_CLOSE));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadPartialWriteThenClose(server, Protocol.PB));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + PB_FULL_WRITE_STAY_OPEN ));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadWriteStayOpen(server, Protocol.PB));
        
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", startingPort + PB_PARTIAL_WRITE_STAY_OPEN));
        server.configureBlocking(false); 
        server.register(selector, SelectionKey.OP_ACCEPT); 
        key = server.keyFor(selector);
        key.attach(new AcceptReadPartialWriteStayOpen(server, Protocol.PB));
        
    }
    
    @Override
    public void run() 
    {
        while(selector.isOpen())
        {
            try
            {
                selector.select();
                for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext();)
                {
                    SelectionKey key = i.next(); 
                    i.remove(); 
                    
                    if (key.isAcceptable())
                    {
                        Acceptor h = (Acceptor) key.attachment();
                        SocketChannel client;
                        switch(h.type)
                        {
                            case ACCEPT_THEN_READ:
                                client = h.getServerSocketChannel().accept();
                                client.configureBlocking(false);
                                client.socket().setTcpNoDelay(true); 
                                client.register(selector, SelectionKey.OP_READ);
                                SelectionKey clientKey = client.keyFor(selector);
                                clientKey.attach(h.duplicate());
                                break;
                            case ACCEPT_THEN_CLOSE:
                                client = h.getServerSocketChannel().accept();
                                client.close();
                                break;
                        }
                    }
                    
                    if (key.isReadable()) { 
                        Acceptor h = (Acceptor) key.attachment();
                        try
                        {
                            h.handle(key);
                        }
                        catch (IOException e)
                        {
                            key.channel().close();
                            key.cancel();
                        }
                    } 
                    
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
            catch (ClosedSelectorException e)
            {
                // no op
            }
        }
    }
    
    public synchronized void shutdown() throws IOException
    {
        if (selector.isOpen())
        {      
            for (Iterator<SelectionKey> i = selector.keys().iterator(); i.hasNext();)
            {
                SelectionKey key = i.next(); 
                key.cancel();
                key.channel().close();
            }

            selector.close();
        }
    }
}
