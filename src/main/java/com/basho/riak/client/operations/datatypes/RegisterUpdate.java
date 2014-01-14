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

import com.basho.riak.client.query.crdt.ops.RegisterOp;
import com.basho.riak.client.util.BinaryValue;

public class RegisterUpdate extends DatatypeUpdate
{

    private byte[] value = null;

    public RegisterUpdate(byte[] value)
    {
        this.value = value;
    }

    public RegisterUpdate()
    {
    }

    public RegisterUpdate set(byte[] value)
    {
        this.value = value;
        return this;
    }

    public RegisterUpdate clear()
    {
        this.value = null;
        return this;
    }

    public byte[] get()
    {
        return value;
    }

    @Override
    public RegisterOp getOp()
    {
        return new RegisterOp(BinaryValue.create(value));
    }


}
