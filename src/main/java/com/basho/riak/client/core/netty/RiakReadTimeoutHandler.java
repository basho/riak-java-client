/*
 * Copyright 2015 Basho Technologies Inc.
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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.ReadTimeoutException;

public class RiakReadTimeoutHandler extends ChannelDuplexHandler
{
    private final Logger logger = LoggerFactory.getLogger(RiakReadTimeoutHandler.class);
    private final long timeout;
    private volatile ScheduledFuture<?> timer;

    public RiakReadTimeoutHandler(long timeout)
    {
        this.timeout = timeout;
    }

    @Override
    public void flush(final ChannelHandlerContext ctx) throws Exception
    {
        super.flush(ctx);
        logger.debug("Scheduled Read Timeout hander. id: {}", ctx.channel().hashCode());
        timer = ctx.executor().schedule(new RiakReadTimeoutTask(ctx), timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        cancelTimeoutTimer();
        logger.debug("Canceled Read Timeout hander. id: {}", ctx.channel().hashCode());
        super.channelReadComplete(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception
    {
        super.close(ctx, future);
        cancelTimeoutTimer();
    }

    private void cancelTimeoutTimer()
    {
        if (timer != null && !timer.isDone())
        {
            timer.cancel(false);
        }
    }
}

class RiakReadTimeoutTask implements Runnable
{
    private ChannelHandlerContext ctx;

    RiakReadTimeoutTask(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
    }

    @Override
    public void run()
    {
        if (ctx.channel().isOpen())
        {
            ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
            ctx.close();
        }
    }
}
