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

import com.basho.riak.client.query.crdt.ops.FlagOp;

public class FlagUpdate extends DatatypeUpdate
{

    private boolean flag = false;

    public FlagUpdate(boolean flag)
    {
        this.flag = flag;
    }

    public FlagUpdate()
    {
    }

    public FlagUpdate set(boolean flag)
    {
        this.flag = flag;
        return this;
    }

    public boolean isEnabled()
    {
        return flag;
    }

    @Override
    public FlagOp getOp()
    {
        return new FlagOp(flag);
    }
}
