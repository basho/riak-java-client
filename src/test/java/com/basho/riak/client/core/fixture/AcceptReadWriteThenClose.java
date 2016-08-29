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
package com.basho.riak.client.core.fixture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class AcceptReadWriteThenClose extends Acceptor
{
    public AcceptReadWriteThenClose(ServerSocketChannel server)
    {
        super(server);
    }

    @Override
    Acceptor duplicate()
    {
        return new AcceptReadWriteThenClose(server);
    }

    @Override
    boolean writePb(SelectionKey key) throws IOException
    {
        ByteBuffer bb = ByteBuffer.allocate(pbMessage.getSerializedSize() + 5);
        bb.putInt(pbMessage.getSerializedSize() + 1);
        bb.put(pbCode);
        bb.put(pbMessage.toByteArray());
        ((WritableByteChannel)key.channel()).write(bb);
        return true;
    }

}
