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
package com.basho.riak.client.operations.crdt;

import com.basho.riak.client.query.crdt.types.CrdtSet;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RiakSet extends RiakDatatype<Set<ByteArrayWrapper>>
{

    private Set<ByteArrayWrapper> values;
    private final SetMutation mutation;

    public RiakSet()
    {
        this(new CrdtSet(Collections.EMPTY_LIST), new SetMutation());
    }

    RiakSet(CrdtSet set, SetMutation mutation)
    {
        this.values = new HashSet<ByteArrayWrapper>(set.viewAsSet());
        this.mutation = mutation;
    }

    public SetMutation add(ByteArrayWrapper value)
    {
        return mutation.add(value);
    }

    public SetMutation remove(ByteArrayWrapper value)
    {
        return mutation.remove(value);
    }

    @Override
    public Set<ByteArrayWrapper> view()
    {
        Set<ByteArrayWrapper> current =
            new HashSet<ByteArrayWrapper>(values);
        current.addAll(mutation.getAdds());
        current.removeAll(mutation.getRemoves());
        return Collections.unmodifiableSet(current);
    }

    @Override
    SetMutation getMutation()
    {
        return mutation;
    }

}
