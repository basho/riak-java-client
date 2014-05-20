/*
 * Copyright 2014 Basho Technologies Inc
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

package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class PingOperation extends FutureOperation<Void, Void, Void>
{
    private final Logger logger = LoggerFactory.getLogger(PingOperation.class);

    @Override
    protected Void convert(List<Void> rawResponse) 
    {
        return null;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_PingReq, new byte[0]);
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        return null;
    }

    @Override
    public Void getQueryInfo()
    {
        return null;
    }    
}
