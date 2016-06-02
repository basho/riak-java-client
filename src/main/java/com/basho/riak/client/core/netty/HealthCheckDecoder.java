/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public abstract class HealthCheckDecoder extends ByteToMessageDecoder 
{
    private final Logger logger = LoggerFactory.getLogger(HealthCheckDecoder.class);
    private final HealthCheckFuture future = new HealthCheckFuture();
    
    protected abstract FutureOperation<?,?,?> buildOperation();
    
    @Override
    protected void decode(ChannelHandlerContext chc, ByteBuf in, List<Object> list) throws Exception
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
            }
            else
            {
                byte code = in.readByte();
                byte[] protobuf = new byte[length - 1];
                in.readBytes(protobuf);
                
                chc.channel().pipeline().remove(this);
                if (code == RiakMessageCodes.MSG_ErrorResp)
                {
                    logger.debug("Received MSG_ErrorResp reply to healthcheck");
                    future.setException((riakErrorToException(protobuf)));
                }
                else
                {
                    logger.debug("Healthcheck op successful; returned code {}", code);
                    future.setMessage(new RiakMessage(code,protobuf));
                }
            }
        }
    }
    
    private void init(ChannelHandlerContext ctx) throws InterruptedException
    {
        ChannelFuture writeAndFlush = ctx.channel().writeAndFlush(buildOperation().channelMessage());
    }
    
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("HealthCheckDecoder Handler Added");
        if (ctx.channel().isActive())
        {
            init(ctx);
        }
        else
        {
            future.setException(new IOException("HealthCheckDecoder added to inactive channel"));
        }
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("HealthCheckDecoder Channel Active");
        init(ctx);
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("Healthcheck channel went inactive");
        future.setException(new IOException("Channel closed while performing health check op."));
        ctx.fireChannelInactive();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception 
    {
        future.setException(new IOException("Exception in channel while performing health check op.", cause));
    }
    
    public RiakFuture<RiakMessage, Void> getFuture()
    {
        return future;
    }
    
    private RiakResponseException riakErrorToException(byte[] protobuf)
    {
        try
        {
            RiakPB.RpbErrorResp error = RiakPB.RpbErrorResp.parseFrom(protobuf);
            return new RiakResponseException(error.getErrcode(), error.getErrmsg().toStringUtf8());
        }
        catch (InvalidProtocolBufferException ex)
        {
            return null;
        }
    }
    
    public static class HealthCheckFuture implements RiakFuture<RiakMessage, Void>
    {

        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Throwable exception;
        private volatile RiakMessage message;
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return latch.getCount() != 1;
        }

        @Override
        public RiakMessage get() throws InterruptedException, ExecutionException
        {
            latch.await();

            if (exception != null)
            {
                throw new ExecutionException(exception);
            }
            else
            {
                return message;
            }
        }

        @Override
        public RiakMessage get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            boolean succeed = latch.await(timeout, unit);
            if (!succeed)
            {
                throw new TimeoutException();
            }
            else if (exception != null)
            {
                throw new ExecutionException(exception);
            }
            else
            {
                return message;
            }
        }
        
        public void setException(Throwable e)
        {
            exception = e;
            latch.countDown();
        }
        
        public void setMessage(RiakMessage m)
        {
            message = m;
            latch.countDown();
        }

        @Override
        public void await() throws InterruptedException
        {
            latch.await();
        }

        @Override
        public void await(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
        }

        @Override
        public RiakMessage getNow()
        {
            return message;
        }

        @Override
        public boolean isSuccess()
        {
            return isDone() && exception == null;
        }

        @Override
        public Throwable cause()
        {
            return exception;
        }

        @Override
        public Void getQueryInfo()
        {
            return null;
        }

        @Override
        public void addListener(RiakFutureListener<RiakMessage, Void> listener)
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void removeListener(RiakFutureListener<RiakMessage, Void> listener)
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
}