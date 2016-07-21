package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.RiakResponseListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakResponseHandler extends ChannelInboundHandlerAdapter
{
    private RiakResponseListener listener;

    public RiakResponseHandler(RiakResponseListener listener)
    {
        super();
        this.listener = listener;
    }

    @Override
    public void channelRead(ChannelHandlerContext chc, Object message) throws Exception
    {
        RiakMessage riakMessage = (RiakMessage) message;
        if (riakMessage.isRiakError())
        {
            listener.onRiakErrorResponse(chc.channel(), riakMessage.getRiakError());
        }
        else
        {
            listener.onSuccess(chc.channel(), riakMessage);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception
    {
        // On any exception in the pipeline we explitly close the context here
        // so the channel doesn't get reused by the ConnectionPool.
        listener.onException(ctx.channel(), cause);
        ctx.close();
    }
}