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

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <ReturnType> The type returned by the streaming and non-streaming operation versions
 * @param <ResponseType> The protocol type returned
 * @param <QueryInfoType> Query info type
 * @since 2.1.0
 */
public abstract class StreamingFutureOperation<ReturnType, ResponseType, QueryInfoType>
        extends FutureOperation<ReturnType, ResponseType, QueryInfoType>
        implements StreamingRiakFuture<ReturnType, QueryInfoType>
{
    private boolean streamResults;

    protected StreamingFutureOperation(boolean streamResults)
    {
        this.streamResults = streamResults;
    }

    @Override
    protected void processMessage(ResponseType decodedMessage)
    {
        if (!streamResults)
        {
            super.processMessage(decodedMessage);
            return;
        }

        processStreamingChunk(decodedMessage);
    }

    abstract protected void processStreamingChunk(ResponseType rawResponseChunk);
}
