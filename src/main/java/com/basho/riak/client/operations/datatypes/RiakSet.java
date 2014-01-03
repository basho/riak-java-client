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
package com.basho.riak.client.operations.datatypes;

import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.query.crdt.types.CrdtSet;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RiakSet extends RiakDatatype<Set<byte[]>>
{

    private final CrdtSet set;

    public RiakSet(CrdtSet set)
    {
        this.set = set;
    }

    @Override
    public Set<byte[]> view()
    {
        Set<byte[]> rset = new HashSet<byte[]>();
        for (ByteArrayWrapper entry : set.viewAsSet())
        {
            rset.add(entry.getValue());
        }
        return Collections.unmodifiableSet(rset);
    }

}
