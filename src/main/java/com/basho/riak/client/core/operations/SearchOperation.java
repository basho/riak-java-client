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
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.basho.riak.protobuf.RiakSearchPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Iterator;
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
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class SearchOperation extends FutureOperation<SearchOperation.Response, RiakSearchPB.RpbSearchQueryResp>
{

    private final RiakSearchPB.RpbSearchQueryReq.Builder reqBuilder;

    private SearchOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected SearchOperation.Response convert(List<RiakSearchPB.RpbSearchQueryResp> rawResponse) throws ExecutionException
    {
        // This isn't a streaming op, there will only be one protobuf
        RiakSearchPB.RpbSearchQueryResp resp = rawResponse.get(0);
        List<Map<String, String>> docList = new LinkedList<Map<String, String>>();
        for (RiakSearchPB.RpbSearchDoc pbDoc : resp.getDocsList())
        {
            Map<String, String> map = new HashMap<String, String>();
            for (RpbPair pair : pbDoc.getFieldsList())
            {
                map.put(pair.getKey().toStringUtf8(), pair.getValue().toStringUtf8());
            }
            docList.add(map);
        }
        return new Response(docList, resp.getMaxScore(), resp.getNumFound());

    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakSearchPB.RpbSearchQueryReq req = reqBuilder.build();
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

    public static class Builder
    {

        private final BinaryValue indexName;
        private final String queryString;
        private final RiakSearchPB.RpbSearchQueryReq.Builder reqBuilder =
            RiakSearchPB.RpbSearchQueryReq.newBuilder();

        public Builder(BinaryValue indexName, String queryString)
        {
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }
            if (null == queryString || queryString.length() == 0)
            {
                throw new IllegalArgumentException("Query string cannot be null or zero length");
            }

            this.indexName = indexName;
            this.queryString = queryString;
            reqBuilder.setIndex(ByteString.copyFrom(indexName.unsafeGetValue()));
            reqBuilder.setQ(ByteString.copyFromUtf8(queryString));
        }

        /**
         * Specify the maximum number of results to return.
         * Riak defaults to 10 if this is not asSet.
         *
         * @param rows the maximum number of results to return.
         * @return a reference to this object.
         */
        public Builder withNumRows(int rows)
        {
            if (rows <= 0)
            {
                throw new IllegalArgumentException("Rows must be >= 1");
            }
            reqBuilder.setRows(rows);
            return this;
        }

        /**
         * Specify the starting result of the query.
         * Useful for pagination. The default is 0.
         *
         * @param start the index of the starting result.
         * @return a reference to this object.
         */
        public Builder withStart(int start)
        {
            if (start < 0)
            {
                throw new IllegalArgumentException("Start must be >= 0");
            }
            reqBuilder.setStart(start);
            return this;
        }

        /**
         * Sort the results on the specified field name.
         * Default is “none”, which causes the results to be sorted in descending order by score.
         *
         * @param fieldName the fieldname to sort the results on.
         * @return a reference to this object.
         */
        public Builder withSortField(String fieldName)
        {
            stringCheck(fieldName);
            reqBuilder.setSort(ByteString.copyFromUtf8(fieldName));
            return this;
        }

        /**
         * Filters the search by an additional query scoped to inline fields.
         *
         * @param filterQuery the filter query.
         * @return a reference to this object.
         */
        public Builder withFilterQuery(String filterQuery)
        {
            stringCheck(filterQuery);
            reqBuilder.setFilter(ByteString.copyFromUtf8(filterQuery));
            return this;
        }

        /**
         * Use the provided field as the default.
         * Overrides the “default_field” setting in the schema file.
         *
         * @param fieldName the name of the field.
         * @return a reference to this object.
         */
        public Builder withDefaultField(String fieldName)
        {
            stringCheck(fieldName);
            reqBuilder.setDf(ByteString.copyFromUtf8(fieldName));
            return this;
        }

        /**
         * Set the default operation.
         * Allowed settings are either “and” or “or”.
         * Overrides the “default_op” setting in the schema file.
         *
         * @param op A string containing either "and" or "or".
         * @return a reference to this object.
         */
        public Builder withDefaultOperation(String op)
        {
            stringCheck(op);
            reqBuilder.setOp(ByteString.copyFromUtf8(op));
            return this;
        }

        /**
         * Only return the provided fields.
         * Filters the results to only contain the provided fields.
         *
         * @param fields a list of field names.
         * @return a reference to this object.
         */
        public Builder withReturnFields(List<String> fields)
        {
            for (String f : fields)
            {
                stringCheck(f);
                reqBuilder.addFl(ByteString.copyFromUtf8(f));
            }
            return this;
        }

        /**
         * Sorts all of the results by bucket key, or the search score, before the given rows are chosen.
         * This is useful when paginating to ensure the results are returned in a consistent order.
         *
         * @param presort a String containing either "key" or "score".
         * @return a reference to this object.
         */
        public Builder withPresort(String presort)
        {
            stringCheck(presort);
            reqBuilder.setPresort(ByteString.copyFromUtf8(presort));
            return this;
        }

        private void stringCheck(String arg)
        {
            if (null == arg || arg.length() == 0)
            {
                throw new IllegalArgumentException("Arguemt cannot be null or zero length");
            }
        }

        public SearchOperation build()
        {
            return new SearchOperation(this);
        }
    }

    public static class Response implements Iterable
    {
        private final List<Map<String, String>> results;
        private final float maxScore;
        private final int numResults; 

        Response(List<Map<String,String>> results, float maxScore, int numResults)
        {
            this.results = results;
            this.maxScore = maxScore;
            this.numResults = numResults;
        }

        @Override
        public Iterator<Map<String,String>> iterator()
        {
            return results.iterator();
        }

        /**
         * Returns the max score from the search query.
         * @return the max score.
         */
        public float getMaxScore()
        {
            return maxScore;
        }

        /**
         * Returns the number of results from the search query.
         * @return the number of results.
         */
        public int numResults()
        {
            return numResults;
        }

        /**
         * Returns the entire list of results from the search query.
         * @return a list containing all the result sets. 
         */
        public List<Map<String,String>> getAllResults()
        {
            return results;
        }
        
    }
    
}
