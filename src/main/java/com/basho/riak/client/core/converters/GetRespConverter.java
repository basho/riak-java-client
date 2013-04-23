/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.core.converters;

import com.basho.riak.client.RiakObject;
import io.netty.handler.codec.http.HttpResponse;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */

public class GetRespConverter implements RiakResponseConverter<RiakObject>
{

    @Override
    public RiakObject convert(HttpResponse response, byte[] content)
    {
        return new RiakObject(new String(content));
    }

    @Override
    public RiakObject convert(byte pbMessageCode, byte[] data)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}