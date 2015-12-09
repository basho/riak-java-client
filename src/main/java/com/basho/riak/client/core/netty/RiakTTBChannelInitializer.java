package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakResponseListener;
import com.basho.riak.client.core.util.Constants;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Created by srg on 12/9/15.
 */
public class RiakTTBChannelInitializer extends RiakChannelInitializer {
    public RiakTTBChannelInitializer(RiakResponseListener listener) {
        super(listener);
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception
    {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LoggingHandler(LogLevel.WARN));
        super.initChannel(ch);
        p.replace(Constants.MESSAGE_CODEC, Constants.MESSAGE_CODEC, new RiakTTBCodec());
    }
}
