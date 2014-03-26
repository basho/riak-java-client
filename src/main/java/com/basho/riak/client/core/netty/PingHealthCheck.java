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

package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.HealthCheckFactory;
import com.basho.riak.client.core.operations.PingOperation;

/**
 * HealthCheck that sends a Ping operation to Riak.
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class PingHealthCheck extends HealthCheckDecoder implements HealthCheckFactory
{

    @Override
    protected FutureOperation<?, ?, ?> buildOperation()
    {
        return new PingOperation();
    }

    @Override
    public HealthCheckDecoder makeDecoder()
    {
        return new PingHealthCheck();
    }
    
}
