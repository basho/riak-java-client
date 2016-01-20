package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.google.protobuf.GeneratedMessage;


/**
 * @author Sergey Galkin <sgalkin at gmail dot com>
 */
public abstract class DeferredEncodingOperation<T, U, S> extends PBFutureOperation<T, U, S> {

    protected DeferredEncodingOperation(final byte reqMessageCode,
                                final byte respMessageCode,
                                final GeneratedMessage.Builder<?> reqBuilder,
                                com.google.protobuf.Parser<U> respParser)
    {
        super(reqMessageCode, respMessageCode, reqBuilder, respParser);
    }

    @Override
    protected RiakMessage createChannelMessage() {
        return new RiakMessage(reqMessageCode, reqBuilder);
    }
}
