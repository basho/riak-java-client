package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.codec.TermToBinaryCodec;
import com.basho.riak.client.core.operations.TTBFutureOperation;

class TTBConverters
{
    static class StoreEncoder implements TTBFutureOperation.TTBEncoder<StoreOperation.Builder>
    {
        private byte[] message = null;
        private final StoreOperation.Builder builder;

        StoreEncoder(StoreOperation.Builder builder)
        {
            this.builder = builder;
        }

        @Override
        public byte[] build()
        {
            if (message == null)
            {
                message = TermToBinaryCodec.encodeTsPutRequest(builder.getTableName(), builder.getRows());
            }
            return message;
        }
    }

    static class VoidDecoder implements TTBFutureOperation.TTBParser<Void>
    {
        @Override
        public Void parseFrom(byte[] data)
        {
            return null;
        }
    }

}
