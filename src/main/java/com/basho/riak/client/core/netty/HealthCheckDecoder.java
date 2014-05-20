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
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.DefaultPromise;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public abstract class HealthCheckDecoder extends ByteToMessageDecoder 
{
    private volatile CountDownLatch promiseLatch = new CountDownLatch(1);
    private final Logger logger = LoggerFactory.getLogger(HealthCheckDecoder.class);
    private volatile DefaultPromise<RiakMessage> promise;
    
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
                    promise.tryFailure((riakErrorToException(protobuf)));
                }
                else
                {
                    logger.debug("Healthcheck op successful; returned code {}", code);
                    promise.trySuccess(new RiakMessage(code,protobuf));
                }
            }
        }
    }
    
    private void init(ChannelHandlerContext ctx)
    {
        promise = new DefaultPromise<RiakMessage>(ctx.executor());
        
        promiseLatch.countDown();
        ctx.channel().writeAndFlush(buildOperation().channelMessage());
    }
    
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("HealthCheckDecoder Handler Added");
        if (ctx.channel().isActive())
        {
            init(ctx);
        }
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        logger.debug("HealthCheckDecoder Channel Active");
        init(ctx);
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        promise.tryFailure(new IOException("Channel closed while performing health check op."));
        ctx.fireChannelInactive();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception 
    {
        promise.tryFailure(new IOException("Exception in channel while performing health check op.", cause));
    }
    
    public DefaultPromise<RiakMessage> getPromise() throws InterruptedException {
        promiseLatch.await();
        return promise;
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
}
