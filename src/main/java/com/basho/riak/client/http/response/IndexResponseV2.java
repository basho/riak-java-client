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

import com.basho.riak.client.http.request.IndexRequest;
import com.basho.riak.client.http.util.Multipart;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class IndexResponseV2
{
    private final List<IndexEntry> entries;
    private String continuation;
    private final IndexRequest request;
    
    public IndexResponseV2(IndexRequest request, HttpResponse r) throws JSONException
    {
        this.request = request;
        entries = new ArrayList<IndexEntry>();
        
        if (r != null && r.isSuccess())
        {
            byte[] entireResponse = r.getBody();
            List<Multipart.Part> parts = Multipart.parse(r.getHttpHeaders(), entireResponse);
            for (Multipart.Part part : parts)
            {
                JSONObject obj = new JSONObject(part.getBodyAsString());
                if (obj.has("keys"))
                {
                    JSONArray jArray = obj.getJSONArray("keys");
                    for (int i = 0; i < jArray.length(); i++)
                    {
                        if (request.isReturnTerms())
                        {
                            entries.add(new IndexEntry(request.getIndexKey(), jArray.getString(i)));
                        }
                        else
                        {
                            entries.add(new IndexEntry(jArray.getString(i)));
                        }
                    }
                }
                else if (obj.has("results"))
                {
                    JSONArray jArray = obj.getJSONArray("results");
                    for (int i = 0; i < jArray.length(); i++)
                    {
                        JSONObject rObj = jArray.getJSONObject(i);
                        String indexValue = rObj.keys().next().toString();
                        entries.add(new IndexEntry(indexValue, rObj.getString(indexValue)));
                    }
                }
                else if (obj.has("continuation"))
                {
                    continuation = obj.getString("continuation");
                }
            }
        }
    }
    
    public List<IndexEntry> getEntries() 
    {
        return entries;
    }
    
    public boolean hasContinuation()
    {
        return continuation != null;
    }
    
    public String getContinuation()
    {
        return continuation;
    }
}
