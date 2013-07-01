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
package com.basho.riak.pbc;

import com.basho.riak.protobuf.RiakKvPB.RpbIndexResp;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class IndexSource extends RiakStreamClient<IndexEntry>
{
    private IndexRequest request;
    private int index;
    private RpbIndexResp pbResponse;
    
    public IndexSource(RiakClient client, RiakConnection conn, IndexRequest request) throws IOException
    {
        super(client, conn);
        this.request = request;
        getNextResponse();
    }
    
    @Override
    public boolean hasNext() throws IOException
    {
        if (isClosed())
        {
            return false;
        }
        
        if (responseIsExhausted())
        {
            getNextResponse();
        }
        
        return !isClosed();
    }

    @Override
    public IndexEntry next() throws IOException
    {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }
        
        if (request.returnTerms())
        {
            RpbPair pair = pbResponse.getResults(index++);
            return new IndexEntry(pair.getKey(), pair.getValue());
        }
        else
        {
            return new IndexEntry(pbResponse.getKeys(index++));
        }
        
    }
    
    private boolean responseIsExhausted()
    {
        if (request.returnTerms())
        {
            return index == pbResponse.getResultsCount();
        }
        else
        {
            return index == pbResponse.getKeysCount();
        }
    }
    
    private void getNextResponse() throws IOException
    {
        if (isClosed())
        {
            return;
        }
        
        // either we're in the first call (r == null)
        // or we got here because we ran out of keys.
        assert pbResponse == null || responseIsExhausted();
        
        do
        {
            if (pbResponse != null)
            {
                if (pbResponse.hasDone() && pbResponse.getDone())
                {
                    if (pbResponse.hasContinuation())
                    {
                        continuation = pbResponse.getContinuation();
                    }
                    close();
                    return;
                }
            }
            
            try
            {
                byte[] data = conn.receive(RiakMessageCodes.MSG_IndexResp);
                if (null == data)
                {
                    close();
                    throw new IOException("Received empty response");
                }
                
                pbResponse = RpbIndexResp.parseFrom(data);
                index = 0;
            }
            catch (IOException e)
            {
                close();
                throw e;
            }
            
        }
        while (pbResponse.getKeysCount() == 0 && pbResponse.getResultsCount() == 0);
        
        
    }
    
    
}
