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
package com.basho.riak.client.core;

import com.basho.riak.client.core.netty.HttpChannelInitializer;
import com.basho.riak.client.core.netty.PbChannelInitializer;
import com.basho.riak.client.core.netty.RiakHttpMessageHandler;
import com.basho.riak.client.core.netty.RiakPbMessageHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public enum Protocol
{
    PB
    {
        private final int DEFAULT_PB_PORT = 8087;
        
        @Override
        int defaultPort()
        {
            return DEFAULT_PB_PORT;
        }
        
        @Override
        ChannelInitializer<SocketChannel> channelInitializer()
        {
            return new PbChannelInitializer();
        }
        
        @Override
        ChannelHandler responseHandler(RiakResponseListener listener)
        {
            return new RiakPbMessageHandler(listener);
        }
        
        
    },
    HTTP
    {
        private final int DEFAULT_HTTP_PORT = 8098;
        
        @Override
        int defaultPort()
        {
            return DEFAULT_HTTP_PORT;
        }
        
        @Override
        ChannelInitializer<SocketChannel> channelInitializer()
        {
            return new HttpChannelInitializer();
        }
        
        @Override
        ChannelHandler responseHandler(RiakResponseListener listener)
        {
            return new RiakHttpMessageHandler(listener);
        }
        
    };
    
    abstract int defaultPort();
    abstract ChannelInitializer<SocketChannel> channelInitializer();
    abstract ChannelHandler responseHandler(RiakResponseListener listener);
}
