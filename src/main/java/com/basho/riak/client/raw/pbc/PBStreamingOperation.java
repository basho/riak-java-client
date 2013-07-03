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
package com.basho.riak.client.raw.pbc;

import com.basho.riak.client.query.RiakStreamingRuntimeException;
import com.basho.riak.client.query.StreamingOperation;
import com.basho.riak.pbc.RiakStreamClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public abstract class PBStreamingOperation<S,T> implements StreamingOperation<T>
{

    final protected RiakStreamClient<S> client;
    
    public PBStreamingOperation(RiakStreamClient<S> client)
    {
        this.client = client;
    }
    
    public List<T> getAll() 
    {
        List<T> list = new ArrayList<T>();
        while (hasNext())
        {
            list.add(next());
        }
        return list;
    }

    public void cancel()
    {
        client.close();
    }

    public Iterator<T> iterator()
    {
        return this;
    }

    public abstract T next();
    
    public boolean hasNext()
    {
        boolean hasNext = false;
        try
        {
            hasNext = client.hasNext();
        }
        catch (IOException ex)
        {
            throw new RiakStreamingRuntimeException(ex);
        }
        
        return hasNext;
            
    }

    public void remove()
    {
        throw new UnsupportedOperationException("Not supported");
    }

    public boolean hasContinuation()
    {
        return client.hasContinuation();
    }

    public String getContinuation()
    {
        return client.getContinuation() == null ? null : client.getContinuation().toStringUtf8();
    }

    

    
    
}
