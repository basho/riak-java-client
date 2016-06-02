/*
 * Copyright 2014 Basho Technologies Inc.
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

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.util.Constants;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakSecurityDecoder extends ByteToMessageDecoder 
    
{
    private final CountDownLatch promiseLatch = new CountDownLatch(1);
    private final SSLEngine sslEngine;
    private final String username;
    private final String password;
    private final Logger logger = LoggerFactory.getLogger(RiakSecurityDecoder.class);
    private volatile DefaultPromise<Void> promise;
    
    private enum State { TLS_START, TLS_WAIT, SSL_WAIT, AUTH_WAIT }
    
    private volatile State state = State.TLS_START;
    
    public RiakSecurityDecoder(SSLEngine engine, String username, String password)
    {
        this.sslEngine = engine;
        this.username = username;
        this.password = password;
    }
    
    @Override
    protected void decode(ChannelHandlerContext chc, ByteBuf in, List<Object> out) throws Exception
    {
        logger.debug("RiakSecurityDecoder decode");
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
                
                switch(state)
                {
                    case TLS_WAIT:
                        switch(code)
                        {
                            case RiakMessageCodes.MSG_StartTls:
                                logger.debug("Received MSG_RpbStartTls reply");
                                // change state
                                this.state = State.SSL_WAIT;
                                // insert SSLHandler
                                SslHandler sslHandler = new SslHandler(sslEngine);
                                // get promise
                                Future<Channel> hsFuture = sslHandler.handshakeFuture();
                                // register callback
                                hsFuture.addListener(new SslListener());
                                // Add handler
                                chc.channel().pipeline().addFirst(Constants.SSL_HANDLER, sslHandler);
                                break;
                            case RiakMessageCodes.MSG_ErrorResp:
                                logger.debug("Received MSG_ErrorResp reply to startTls");
                                promise.tryFailure((riakErrorToException(protobuf)));
                                break;
                            default:
                                promise.tryFailure(new RiakResponseException(0,
                                    "Invalid return code during StartTLS; " + code));
                        }
                        break;
                    case AUTH_WAIT:
                        chc.channel().pipeline().remove(this);
                        switch(code)
                        {
                            case RiakMessageCodes.MSG_AuthResp:
                                logger.debug("Received MSG_RpbAuthResp reply");
                                promise.trySuccess(null);
                                break;
                            case RiakMessageCodes.MSG_ErrorResp:
                                logger.debug("Received MSG_ErrorResp reply to auth");
                                promise.tryFailure(riakErrorToException(protobuf));
                                break;
                            default:
                                promise.tryFailure(new RiakResponseException(0,
                                    "Invalid return code during Auth; " + code));
                        }
                        break;
                    default:
                        // WTF?
                        logger.error("Received message while not in TLS_WAIT or AUTH_WAIT");
                        promise.tryFailure(new IllegalStateException("Received message while not in TLS_WAIT or AUTH_WAIT"));
                }
            }
        }
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
    
    private void init(ChannelHandlerContext ctx)
    {
        state = State.TLS_WAIT;
        promise = new DefaultPromise<Void>(ctx.executor());
        promiseLatch.countDown();
        ctx.channel().writeAndFlush(new RiakMessage(RiakMessageCodes.MSG_StartTls, 
                                    new byte[0]));
    }
    
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        
        logger.debug("Handler Added");
        if (ctx.channel().isActive())
        {
            init(ctx);
        }
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("Channel Active");
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("Channel Inactive");

        promise.tryFailure(new IOException("Channel closed during auth"));
        ctx.fireChannelInactive();
       
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception 
    {
        logger.debug("Exception Caught: {}", cause);

        if (cause.getCause() instanceof javax.net.ssl.SSLHandshakeException)
        {
            // consume
        }
        else
        {
            ctx.fireExceptionCaught(cause);
        }
    }
    
    public DefaultPromise<Void> getPromise() throws InterruptedException
    {
        promiseLatch.await();
        return promise;
    }
    
    private class SslListener implements GenericFutureListener<Future<Channel>>
    {
        @Override
        public void operationComplete(Future<Channel> future) throws Exception
        {
            if (future.isSuccess())
            {
                logger.debug("SSL Handshake success!");
                Channel c = future.getNow();
                state = State.AUTH_WAIT;
                RiakPB.RpbAuthReq authReq = 
                RiakPB.RpbAuthReq.newBuilder()
                    .setUser(ByteString.copyFromUtf8(username))
                    .setPassword(ByteString.copyFromUtf8(password))
                    .build();
                c.writeAndFlush(new RiakMessage(RiakMessageCodes.MSG_AuthReq, 
                                authReq.toByteArray()));
                
            }
            else
            {
                logger.error("SSL Handshake failed: ", future.cause());
                promise.tryFailure(future.cause());
            }
        }
    }
    
    
    
}
