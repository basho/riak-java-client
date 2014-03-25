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

package com.basho.riak.client.core;

import com.basho.riak.client.core.netty.HealthCheckDecoder;

/**
 * Provides a simple factory method for HealhCheckDecoders.
 * <p>
 * Because of the stateful nature of the codec used for performing health checks,
 * a new instance needs to be created each time. When configuring a RiakNode, a 
 * HealthCheckFactory is suppled for this purpose.
 * <p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public interface HealthCheckFactory
{
    HealthCheckDecoder makeDecoder();
}
