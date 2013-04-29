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
package com.basho.riak.client.util.http;

import java.util.Random;
import javax.xml.bind.DatatypeConverter;

/**
 * If using HTTP without vnode_vclocks enabled in Riak the client must
 * uniquely identify itself.
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public final class ClientIdGenerator
{
    public static String generateClientId()
    {
        byte[] rnd = new byte[4];
        new Random().nextBytes(rnd);
        return Base64.encodeToString(rnd, true);
    }
}
