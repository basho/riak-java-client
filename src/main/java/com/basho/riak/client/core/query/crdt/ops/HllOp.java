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
package com.basho.riak.client.core.query.crdt.ops;

import com.basho.riak.client.core.util.BinaryValue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class HllOp implements CrdtOp
{
    private final Set<BinaryValue> elements = new HashSet<>();

    public HllOp() {}

    public HllOp(Collection<BinaryValue> elements)
    {
        this.elements.addAll(elements);
    }

    public HllOp add(BinaryValue element)
    {
        this.elements.add(element);
        return this;
    }

    public HllOp addAll(Collection<BinaryValue> elements)
    {
        this.elements.addAll(elements);
        return this;
    }

    public Set<BinaryValue> getElements()
    {
        return elements;
    }

    @Override
    public String toString()
    {
        return "{Elements: " + elements + "}";
    }
}
