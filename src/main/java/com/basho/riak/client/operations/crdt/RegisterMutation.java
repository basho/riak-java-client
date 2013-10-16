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

import com.basho.riak.client.query.crdt.ops.CrdtOp;
import com.basho.riak.client.query.crdt.ops.RegisterOp;
import com.basho.riak.client.util.ByteArrayWrapper;

public class RegisterMutation extends CrdtMutation
{

    private ByteArrayWrapper value = null;

    RegisterMutation(ByteArrayWrapper value)
    {
        this.value = value;
    }

    RegisterMutation()
    {
    }

    public static RegisterMutation registerValue(ByteArrayWrapper value)
    {
        return new RegisterMutation(value);
    }

    public static RegisterMutation emptyRegister()
    {
        return new RegisterMutation();
    }

    public RegisterMutation set(ByteArrayWrapper value)
    {
        this.value = value;
        return this;
    }

    public RegisterMutation clear()
    {
        this.value = null;
        return this;
    }

    public ByteArrayWrapper get()
    {
        return value;
    }

    @Override
    public CrdtOp getOp()
    {
        return new RegisterOp(value);
    }


}
