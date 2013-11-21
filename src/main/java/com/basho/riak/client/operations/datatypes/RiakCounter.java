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

import com.basho.riak.client.query.crdt.types.CrdtCounter;

public class RiakCounter extends RiakDatatype<Long>
{

    private final long initialValue;
    private final CounterMutation mutation;

    public RiakCounter()
    {
        this(new CrdtCounter(0), new CounterMutation());
    }

    public RiakCounter(CrdtCounter counter)
    {
        this(counter, new CounterMutation());
    }

    RiakCounter(CrdtCounter counter, CounterMutation mutation)
    {
        this.initialValue = counter.getValue();
        this.mutation = mutation;
    }

    public void increment(long amount)
    {
        mutation.increment(amount);
    }

    public void increment()
    {
        mutation.increment(1);
    }

    public void decrement()
    {
        mutation.increment(-1);
    }

    @Override
    public Long view()
    {
        return initialValue + mutation.getDelta();
    }

    @Override
    CounterMutation getMutation()
    {
        return mutation;
    }

}
