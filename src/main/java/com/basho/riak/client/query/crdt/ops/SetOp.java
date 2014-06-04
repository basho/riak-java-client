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

import com.basho.riak.client.util.BinaryValue;

import java.util.HashSet;
import java.util.Set;

public class SetOp implements CrdtOp
{

    private final Set<BinaryValue> adds = new HashSet<BinaryValue>();
    private final Set<BinaryValue> removes = new HashSet<BinaryValue>();

    public SetOp(Set<BinaryValue> adds, Set<BinaryValue> removes)
    {
        this.adds.addAll(adds);
        this.removes.addAll(removes);
    }

    public SetOp() {}

    public SetOp add(BinaryValue element)
    {
        this.adds.add(element);
        return this;
    }

    public SetOp remove(BinaryValue element)
    {
        this.removes.add(element);
        return this;
    }

    public Set<BinaryValue> getAdds()
    {
        return adds;
    }

    public Set<BinaryValue> getRemoves()
    {
        return removes;
    }
    
    @Override
    public String toString()
    {
        return "Add: " + adds + " Remove: " + removes;
    }
}
