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
package com.basho.riak.client.raw.http;

import com.basho.riak.client.IndexEntry;
import com.basho.riak.client.http.response.IndexResponseV2;
import com.basho.riak.client.query.StreamingOperation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class IndexSource implements StreamingOperation<IndexEntry>
{

    private final List<IndexEntry> entryList;
    private final String continuation;
    private final Iterator<IndexEntry> iterator;
    
    public IndexSource(IndexResponseV2 indexResponse)
    {
        entryList = new ArrayList<IndexEntry>();
        for (com.basho.riak.client.http.response.IndexEntry entry : indexResponse.getEntries())
        {
            entryList.add(new IndexEntry(entry.getIndexValue(), entry.getObjectKey()));
        }
        continuation = indexResponse.getContinuation();
        iterator = entryList.iterator();
    }

    public List<IndexEntry> getAll()
    {
        return entryList;
    }

    public void cancel()
    {
        // No op because this isn't really streaming.
    }

    public boolean hasContinuation()
    {
        return continuation != null;
    }

    public String getContinuation()
    {
        return continuation;
    }

    public Iterator<IndexEntry> iterator()
    {
        return entryList.iterator();
    }

    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    public IndexEntry next()
    {
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException("Not supported");
    }
    
    
    
}
