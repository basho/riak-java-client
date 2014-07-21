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
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * A health check that attempts to store a value in Riak.
 * <p>
 * <b>Important note when using this HealthCheckDecoder:</b>
 * </p>
 * <p>
 * If security is enabled in Riak, the user the client is configured 
 * for must have access rights to the supplied location. 
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class StoreValueHealthCheck extends HealthCheckDecoder implements HealthCheckFactory
{
    private final Location location;
    private final BinaryValue value;
      
    
    public StoreValueHealthCheck(Location location, BinaryValue value)
    {
        this.location = location;
        this.value = value;
    }
    
    @Override
    protected FutureOperation<?, ?, ?> buildOperation()
    {
        RiakObject ro = new RiakObject().setValue(value);
        StoreOperation op = new StoreOperation.Builder(location).withContent(ro).build();
        return op;
    }

    @Override
    public HealthCheckDecoder makeDecoder()
    {
        return new StoreValueHealthCheck(location, value);
    }
    
}
