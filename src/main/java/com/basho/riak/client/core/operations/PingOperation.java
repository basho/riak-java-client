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

package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.util.RiakMessageCodes;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class PingOperation extends FutureOperation<PingOperation.Response, Void, Void>
{
    private final Logger logger = LoggerFactory.getLogger(PingOperation.class);

    @Override
    protected Response convert(List<Void> rawResponse) 
    {
        return new Response(true);
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
    protected Void getQueryInfo()
    {
        return null;
    }
    
    public static class Response
    {
        private final boolean succeeded;
        
        Response(boolean succeeded)
        {
            this.succeeded = succeeded;
        }
        
        public boolean isSuccessful()
        {
            return succeeded;
        }
    }
}
