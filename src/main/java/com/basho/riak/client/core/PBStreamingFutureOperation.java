/*
 * Copyright 2016 Basho Technologies, Inc.
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

package com.basho.riak.client.core;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.GeneratedMessage;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <ReturnType> The type returned by the streaming and non-streaming operation versions
 * @param <ResponseType> The protocol type returned
 * @param <QueryInfoType> Query info type
 * @since 2.1.0
 */
public abstract class PBStreamingFutureOperation<ReturnType, ResponseType, QueryInfoType>
        extends PBFutureOperation<ReturnType, ResponseType, QueryInfoType>
        implements StreamingRiakFuture<ReturnType, QueryInfoType>
{
    private final TransferQueue<ReturnType> responseQueue;
    private boolean streamResults;

    protected PBStreamingFutureOperation(final byte reqMessageCode,
                                         final byte respMessageCode,
                                         final GeneratedMessage.Builder<?> reqBuilder,
                                         com.google.protobuf.Parser<ResponseType> respParser,
                                         boolean streamResults)
    {
        super(reqMessageCode, respMessageCode, reqBuilder, respParser);
        this.streamResults = streamResults;
        this.responseQueue = new LinkedTransferQueue<>();
    }

    @Override
    protected void processMessage(ResponseType decodedMessage)
    {
        if (!streamResults)
        {
            super.processMessage(decodedMessage);
            return;
        }

        final ReturnType r = processStreamingChunk(decodedMessage);
        responseQueue.offer(r);
    }

    abstract protected ReturnType processStreamingChunk(ResponseType rawResponseChunk);

    public final TransferQueue<ReturnType> getResultsQueue()
    {
        return this.responseQueue;
    }
}
