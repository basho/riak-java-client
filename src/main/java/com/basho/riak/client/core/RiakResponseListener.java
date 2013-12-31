/*
 * Copyright Basho Technologies Inc.
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

import com.basho.riak.client.core.netty.RiakResponseException;
import io.netty.channel.Channel;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public interface RiakResponseListener
{
    public void onSuccess(Channel channel, RiakMessage response);
    public void onRiakErrorResponse(Channel channel, RiakResponseException response); 
    public void onException(Channel channel, Throwable t);
}
