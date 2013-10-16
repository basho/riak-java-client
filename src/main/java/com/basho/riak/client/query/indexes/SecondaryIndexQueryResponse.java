/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.ArrayList;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @Since 2.0
 */
public class SecondaryIndexQueryResponse extends ArrayList<SecondaryIndexEntry>
{
    private ByteArrayWrapper continuation;
    
    public void setContinuation(ByteArrayWrapper continuation)
    {
        this.continuation = continuation;
    }
    
    public boolean hasContinuation()
    {
        return continuation != null;
    }
    
    public ByteArrayWrapper getContinuation()
    {
        return continuation;
    }
    
    
    
}
