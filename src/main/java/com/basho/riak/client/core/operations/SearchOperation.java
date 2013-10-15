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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.query.search.SearchResult;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.basho.riak.protobuf.RiakSearchPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A Riak Search or Yokozuna query operation.
 * <p>
 * Due to the nature of both Riak Search and Yokozuna, all Strings must be
 * UTF-8 encoded.
 * </p>
 * <p>
 * This operation will fail if search is not enabled or the index does not exist.
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class SearchOperation extends FutureOperation<SearchResult, RiakSearchPB.RpbSearchQueryResp>
{

    private final RiakSearchPB.RpbSearchQueryReq.Builder builder =
        RiakSearchPB.RpbSearchQueryReq.newBuilder();
    
    /**
     * Constructs a SearchOperation for Riak Search or Yokozuna.
     * In the case of Yokozuna the index name is required to be UTF-8.
     * @param indexName the index name
     * @param queryString the query string
     */
    public SearchOperation(ByteArrayWrapper indexName, String queryString)
    {
        if (null == indexName || indexName.length() == 0)
        {
            throw new IllegalArgumentException("Index name cannot be null or zero length");
        }
        if (null == queryString || queryString.length() == 0)
        {
            throw new IllegalArgumentException("Query string cannot be null or zero length");
        }
        
        builder.setIndex(ByteString.copyFrom(indexName.unsafeGetValue()));
        builder.setQ(ByteString.copyFromUtf8(queryString));
    }
    
    /**
     * Specify the maximum number of results to return.
     * Riak defaults to 10 if this is not set.
     * @param rows the maximum number of results to return.
     * @return a reference to this object.
     */
    public SearchOperation withNumRows(int rows)
    {
        if (rows <= 0)
        {
            throw new IllegalArgumentException("Rows must be >= 1");
        }
        builder.setRows(rows);
        return this;
    }
    
    /**
     * Specify the starting result of the query.
     * Useful for pagination. The default is 0.
     * @param start the index of the starting result.
     * @return a reference to this object. 
     */
    public SearchOperation withStart(int start)
    {
        if (start < 0)
        {
            throw new IllegalArgumentException("Start must be >= 0");
        }
        builder.setStart(start);
        return this;
    }
    
    /**
     * Sort the results on the specified field name.
     * Default is “none”, which causes the results to be sorted in descending order by score.
     * @param fieldName the fieldname to sort the results on.
     * @return a reference to this object. 
     */
    public SearchOperation withSortField(String fieldName)
    {
        stringCheck(fieldName);
        builder.setSort(ByteString.copyFromUtf8(fieldName));
        return this;
    }
    
    /**
     * Filters the search by an additional query scoped to inline fields.
     * @param filterQuery the filter query.
     * @return a reference to this object. 
     */
    public SearchOperation withFilterQuery(String filterQuery)
    {
        stringCheck(filterQuery);
        builder.setFilter(ByteString.copyFromUtf8(filterQuery));
        return this;
    }
    
    /**
     * Use the provided field as the default.
     * Overrides the “default_field” setting in the schema file.
     * @param fieldName the name of the field.
     * @return a reference to this object. 
     */
    public SearchOperation withDefaultField(String fieldName)
    {
        stringCheck(fieldName);
        builder.setDf(ByteString.copyFromUtf8(fieldName));
        return this;
    }
    
    /**
     * Set the default operation.
     * Allowed settings are either “and” or “or”. 
     * Overrides the “default_op” setting in the schema file. 
     * @param op A string containing either "and" or "or".
     * @return a reference to this object. 
     */
    public SearchOperation withDefaultOperation(String op)
    {
        stringCheck(op);
        builder.setOp(ByteString.copyFromUtf8(op));
        return this;
    }
    
    /**
     * Only return the provided fields.
     * Filters the results to only contain the provided fields.
     * @param fields a list of field names.
     * @return a reference to this object. 
     */
    public SearchOperation withReturnFields(List<String> fields)
    {
        for (String f : fields)
        {
            stringCheck(f);
            builder.addFl(ByteString.copyFromUtf8(f));
        }
        return this;
    }
    
    /**
     * Sorts all of the results by bucket key, or the search score, before the given rows are chosen.
     * This is useful when paginating to ensure the results are returned in a consistent order.
     * @param presort a String containing either "key" or "score".
     * @return a reference to this object. 
     */
    public SearchOperation withPresort(String presort)
    {
        stringCheck(presort);
        builder.setPresort(ByteString.copyFromUtf8(presort));
        return this;
    }
    
    private void stringCheck(String arg)
    {
        if (null == arg || arg.length() == 0)
        {
            throw new IllegalArgumentException("Arguemt cannot be null or zero length");
        }
    }
    
    @Override
    protected SearchResult convert(List<RiakSearchPB.RpbSearchQueryResp> rawResponse) throws ExecutionException
    {
        // This isn't a streaming op, there will only be one protobuf
        RiakSearchPB.RpbSearchQueryResp resp = rawResponse.get(0);
        List<Map<String,String>> docList = new LinkedList<Map<String,String>>();
        for (RiakSearchPB.RpbSearchDoc pbDoc : resp.getDocsList())
        {
            Map<String,String> map = new HashMap<String,String>();
            for (RpbPair pair : pbDoc.getFieldsList())
            {
                map.put(pair.getKey().toStringUtf8(), pair.getValue().toStringUtf8());
            }
            docList.add(map);
        }
        return new SearchResult(docList, resp.getMaxScore(), resp.getNumFound());
        
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakSearchPB.RpbSearchQueryReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_SearchQueryReq, req.toByteArray());
    }

    @Override
    protected RiakSearchPB.RpbSearchQueryResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_SearchQueryResp);
        try
        {
            return RiakSearchPB.RpbSearchQueryResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
    
}
