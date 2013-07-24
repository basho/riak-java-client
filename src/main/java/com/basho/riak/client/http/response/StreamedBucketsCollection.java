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
package com.basho.riak.client.http.response;

import com.basho.riak.client.http.util.CollectionWrapper;
import org.json.JSONException;
import org.json.JSONTokener;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class StreamedBucketsCollection extends CollectionWrapper<String>
{
    private JSONTokener tokens;
    boolean readingArray = false;
    
    public StreamedBucketsCollection(JSONTokener tokens) 
    {
        this.tokens = tokens;
    }
    
    @Override
    protected boolean cacheNext()
    {
        if (null == tokens)
        {
            return false;
        }
        else
        {
            try
            {
                while (tokens.more())
                {
                    char c = tokens.nextClean();
                    if ((!readingArray && c == '[') || (readingArray && c == ','))
                    {
                        if (tokens.nextClean() != ']')
                        {
                            tokens.back();
                            readingArray = true;
                            cache(tokens.nextValue().toString());
                            return true;
                        }
                    }
                    else if (readingArray && c == ']')
                    {
                        readingArray =  false;
                    }
                    else if (c == '\\')
                    {
                        tokens.nextClean(); // skip over escaped characters
                    }
                    
                }
            }
            catch (JSONException e) 
            { /* no op */ }
            
            return false;
        }
    }

    @Override
    protected void closeBackend()
    {
        tokens = null;
    }
    
}
