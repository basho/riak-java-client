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
import com.basho.riak.client.query.crdt.ops.FlagOp;

public class FlagMutation extends CrdtMutation
{

    private boolean flag = false;

    FlagMutation(boolean flag)
    {
        this.flag = flag;
    }

    FlagMutation()
    {
    }

    public static FlagMutation newBuilder()
    {
        return new FlagMutation();
    }

    public static FlagMutation enabled()
    {
        return new FlagMutation(true);
    }

    public static FlagMutation disabled()
    {
        return new FlagMutation(false);
    }

    private FlagMutation setFlag(boolean flag)
    {
        this.flag = flag;
        return this;
    }

    private boolean isSet()
    {
        return flag;
    }

    @Override
    public FlagOp getOp()
    {
        return new FlagOp(flag);
    }
}
