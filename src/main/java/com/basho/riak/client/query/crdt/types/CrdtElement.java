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
package com.basho.riak.client.query.crdt.types;

import com.basho.riak.client.util.BinaryValue;

public abstract class CrdtElement
{

    private BinaryValue context = null;

    public void setContext(BinaryValue context)
    {
        this.context = context;
    }

    public boolean hasContext()
    {
        return context != null;
    }

    public BinaryValue getContext()
    {
        return context;
    }

    public boolean isMap()
    {
        return this instanceof CrdtMap;
    }

    public boolean isSet()
    {
        return this instanceof CrdtSet;
    }

    public boolean isCounter()
    {
        return this instanceof CrdtCounter;
    }

    public boolean isRegister()
    {
        return this instanceof CrdtRegister;
    }

    public boolean isFlag()
    {
        return this instanceof CrdtFlag;
    }

    public CrdtMap getAsMap()
    {
        if (!isMap())
        {
            throw new IllegalStateException("This is not an instance of a CrdtMap");
        }
        return (CrdtMap) this;
    }

    public CrdtSet getAsSet()
    {
        if (!isSet())
        {
            throw new IllegalStateException("This is not an instance of a CrdtSet");
        }
        return (CrdtSet) this;
    }

    public CrdtCounter getAsCounter()
    {
        if (!isCounter())
        {
            throw new IllegalStateException("This is not an instance of a CrdtCounter");
        }
        return (CrdtCounter) this;
    }

    public CrdtRegister getAsRegister()
    {
        if (!isRegister())
        {
            throw new IllegalStateException("This is not an instance of a CrdtRegister");
        }
        return (CrdtRegister) this;
    }

    public CrdtFlag getAsFlag()
    {
        if (!isFlag())
        {
            throw new IllegalStateException("This is not an instance of a CrdtFlag");
        }
        return (CrdtFlag) this;
    }



}
