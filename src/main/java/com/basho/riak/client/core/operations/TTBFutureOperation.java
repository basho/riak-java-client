package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;

/**
 * An abstract TTB operation that introduces generic encoding/decoding
 *
 * @author Alex Moore <amoore at basho dot com>
 * @param <T> The type the operation returns
 * @param <S> Query info type

 * @since 2.0.6
 */

public abstract class TTBFutureOperation<T, S> extends FutureOperation<T, byte[], S>
{
    protected final byte reqMessageCode = RiakMessageCodes.MSG_TsTtbMsg;
    protected final byte respMessageCode = RiakMessageCodes.MSG_TsTtbMsg;
    protected final TTBEncoder requestBuilder;
    protected final TTBParser<T> responseParser;

    protected TTBFutureOperation(TTBEncoder requestBuilder, TTBParser<T> responseParser)
    {
        this.requestBuilder = requestBuilder;
        this.responseParser = responseParser;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(reqMessageCode, requestBuilder.build());
    }

    @Override
    protected byte[] decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, respMessageCode);

        byte[] data = rawMessage.getData();

        if (data.length == 0) // not found
        {
            return null;
        }

        return data;
    }

    public interface TTBEncoder
    {
        byte[] build();
    }

    public interface TTBParser<T>
    {
        T parseFrom(byte[] data);
    }
}
