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

import com.basho.riak.client.query.crdt.types.CrdtFlag;

public class RiakFlag extends RiakDatatype<Boolean>
{

    private final FlagMutation mutation;

    public RiakFlag()
    {
        this(new CrdtFlag(false), new FlagMutation());
    }

    RiakFlag(CrdtFlag flag, FlagMutation mutation)
    {
        this.mutation = mutation.setFlag(flag.getEnabled());
    }

    public void setFlag(boolean state)
    {
        mutation.setFlag(state);
    }

    public void enable()
    {
        mutation.setFlag(true);
    }

    public void disable()
    {
        mutation.setFlag(false);
    }

    @Override
    public Boolean view()
    {
        return mutation.getEnabled();
    }

    @Override
    FlagMutation getMutation()
    {
        return mutation;
    }

}
