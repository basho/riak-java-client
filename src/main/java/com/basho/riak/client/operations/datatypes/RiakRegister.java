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

import com.basho.riak.client.query.crdt.types.CrdtRegister;
import com.basho.riak.client.util.ByteArrayWrapper;

public class RiakRegister extends RiakDatatype<ByteArrayWrapper>
{

    private final RegisterMutation mutation;

    public RiakRegister()
    {
        this(new CrdtRegister(null), new RegisterMutation());
    }

    public RiakRegister(ByteArrayWrapper value)
    {
        this(new CrdtRegister(value), new RegisterMutation());
    }

    RiakRegister(CrdtRegister register, RegisterMutation mutation)
    {
        this.mutation = mutation.set(register.getValue());
    }

    public void set(ByteArrayWrapper value)
    {
        mutation.set(value);
    }

    public void unset()
    {
        mutation.clear();
    }

    @Override
    public ByteArrayWrapper view()
    {
        return mutation.get();
    }

    @Override
    RegisterMutation getMutation()
    {
        return mutation;
    }

}
