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

import com.basho.riak.client.IndexEntry;
import com.basho.riak.client.query.RiakStreamingRuntimeException;
import static com.basho.riak.client.raw.pbc.ConversionUtil.nullSafeToStringUtf8;
import com.basho.riak.pbc.RiakStreamClient;
import java.io.IOException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class PBStreamingIndex extends PBStreamingOperation<com.basho.riak.pbc.IndexEntry, IndexEntry>
{

    public PBStreamingIndex(RiakStreamClient<com.basho.riak.pbc.IndexEntry> client)
    {
        super(client);
    }
    
    @Override
    public IndexEntry next()
    {
        try
        {
            com.basho.riak.pbc.IndexEntry pbEntry = client.next();
            return new IndexEntry(nullSafeToStringUtf8(pbEntry.getIndexValue()), 
                                nullSafeToStringUtf8(pbEntry.getObjectKey()));
        }
        catch (IOException ex)
        {
            throw new RiakStreamingRuntimeException(ex);
        }
    }

}
