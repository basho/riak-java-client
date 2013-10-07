/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.query.crdt.ops;

import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Set;

public class SetOp implements CrdtOp
{

    private final Set<ByteArrayWrapper> adds = new HashSet<ByteArrayWrapper>();
    private final Set<ByteArrayWrapper> removes = new HashSet<ByteArrayWrapper>();

    public SetOp(Set<ByteArrayWrapper> adds, Set<ByteArrayWrapper> removes)
    {
        this.adds.addAll(adds);
        this.removes.addAll(removes);
    }

    public SetOp() {}

    public SetOp add(ByteArrayWrapper element)
    {
        this.adds.add(element);
        this.removes.remove(element);
        return this;
    }

    public SetOp remove(ByteArrayWrapper element)
    {
        this.adds.remove(element);
        this.removes.add(element);
        return this;
    }

    public Set<ByteArrayWrapper> getAdds()
    {
        return adds;
    }

    public Set<ByteArrayWrapper> getRemoves()
    {
        return removes;
    }
}
