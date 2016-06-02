package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;

import com.google.protobuf.GeneratedMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.LoggerFactory;

/**
 * An abstract PB operation that introduces generic encoding/decoding
 *
 * @author Sergey Galkin <sgalkin at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @param <T> The type the operation returns
 * @param <U> The protocol type returned
 * @param <S> Query info type

 * @since 2.0.3
 */
public abstract class PBFutureOperation<T, U, S> extends FutureOperation<T, U, S>
{
    protected final Builder<?> reqBuilder;
    private final com.google.protobuf.Parser<U> respParser;
    protected final byte reqMessageCode;
    private final byte respMessageCode;


    protected PBFutureOperation(final byte reqMessageCode,
                                final byte respMessageCode,
                                final Builder<?> reqBuilder,
                                com.google.protobuf.Parser<U> respParser)
    {
        this.reqBuilder = reqBuilder;
        this.respParser = respParser;
        this.reqMessageCode = reqMessageCode;
        this.respMessageCode = respMessageCode;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(reqMessageCode, reqBuilder.build().toByteArray());
    }

    @Override
    protected U decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, respMessageCode);
        try
        {
            byte[] data = rawMessage.getData();

            if (data.length == 0) // not found
            {
                return null;
            }

            return respParser.parseFrom(data);
        }
        catch (InvalidProtocolBufferException e)
        {
            LoggerFactory.getLogger(getClass()).error("Invalid message received '{}', whereas the {} code was expected",
                    rawMessage.getCode(), respMessageCode);

            throw new IllegalArgumentException("Invalid message received", e);
        }
    }
}
