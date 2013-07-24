/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
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
package com.basho.riak.client;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class IndexEntry
{
    private final String indexValue;
    private final String objectKey;
    
    public IndexEntry(String objectKey)
    {
        this(null, objectKey);
    }
    
    public IndexEntry(String indexValue, String objectKey)
    {
        this.indexValue = indexValue;
        this.objectKey = objectKey;        
    }
    
    /**
     * Returns true if The index key is present.
     * @see IndexRequest.Builder#withReturnKeyAndIndex(boolean) 
     * @return true is the index key is present
     */
    public boolean hasIndexValue() 
    {
        return indexValue != null;
    }
    
    /**
     * The index value
     * @return The index value or null if not present
     */
    public String getIndexValue()
    {
        return indexValue;
    }
    
    /**
     * The index value as a long
     * @return the long value
     * @throws NumberFormatException if this is a _bin index
     */
    public long getIndexValueAsLong()
    {
        return Long.parseLong(indexValue);
    }
    
    /**
     * The Riak object's key
     * @return The key for the object in Riak
     */
    public String getObjectKey()
    {
        return objectKey;
    }
}