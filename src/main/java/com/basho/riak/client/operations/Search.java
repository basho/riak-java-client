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
package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.util.BinaryValue;

import java.util.*;
import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class Search extends RiakCommand<SearchOperation.Response, BinaryValue>
{

    public static enum Presort
    {
        KEY("key"), SCORE("score");

        final String sortStr;

        Presort(String sortStr)
        {
            this.sortStr = sortStr;
        }
    }

    private final String index;
    private final String query;
    private final int start;
    private final int rows;
    private final Presort presort;
    private final String filterQuery;
    private final String sortField;
    private final List<String> returnFields;
    private final Map<SearchOption<?>, Object> options =
	    new HashMap<SearchOption<?>, Object>();

    public Search(Builder builder)
    {
        this.index = builder.index;
        this.query = builder.query;
        this.start = builder.start;
        this.rows = builder.rows;
        this.presort = builder.presort;
	    this.filterQuery = builder.filterQuery;
	    this.sortField = builder.sortField;
	    this.returnFields = builder.returnFields;
	    this.options.putAll(builder.options);
    }


    @Override
    protected final SearchOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        RiakFuture<SearchOperation.Response, BinaryValue> future =
            doExecuteAsync(cluster);
        
        future.await();
        
        if (future.isSuccess())
        {
            return future.get();
        }
        else
        {
            throw new ExecutionException(future.cause().getCause());
        }
    }
    
    @Override
    protected RiakFuture<SearchOperation.Response, BinaryValue> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<SearchOperation.Response, BinaryValue> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<SearchOperation.Response, BinaryValue, SearchOperation.Response, BinaryValue> future =
            new CoreFutureAdapter<SearchOperation.Response, BinaryValue, SearchOperation.Response, BinaryValue>(coreFuture)
            {
                @Override
                protected SearchOperation.Response convertResponse(SearchOperation.Response coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected FailureInfo<BinaryValue> convertFailureInfo(FailureInfo<BinaryValue> coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }
    
    private SearchOperation buildCoreOperation()
    {
        SearchOperation.Builder builder = new SearchOperation.Builder(BinaryValue.create(index), query);

        for (Map.Entry<SearchOption<?>, Object> option : options.entrySet())
        {

            if (option.getKey() == SearchOption.DEFAULT_FIELD)
            {
                builder.withDefaultField((String) option.getValue());
            }
            else if (option.getKey() == SearchOption.DEFAULT_OPERATION)
            {
                SearchOption.Operation op = (SearchOption.Operation) option.getValue();
                builder.withDefaultOperation(op.opStr);
            }

        }

        if (start >= 0)
        {
            builder.withStart(start);
        }

        if (rows > 0)
        {
            builder.withNumRows(rows);
        }

        if (presort != null)
        {
            builder.withPresort(presort.sortStr);
        }

        if (filterQuery != null)
        {
            builder.withFilterQuery(filterQuery);
        }

        if (sortField != null)
        {
            builder.withSortField(sortField);
        }

        if (returnFields != null)
        {
            builder.withReturnFields(returnFields);
        }

        return builder.build();
    }

	public static class Builder
	{
		private final String index;
		private final String query;
		private int start;
		private int rows;
		private Presort presort;
		private String filterQuery;
		private String sortField;
		private List<String> returnFields;
		private Map<SearchOption<?>, Object> options = new HashMap<SearchOption<?>, Object>();

		public Builder(String index, String query)
		{
			this.index = index;
			this.query = query;
		}

		public Builder withPresort(Presort presort)
		{
			this.presort = presort;
			return this;
		}

		public Builder withStart(int start)
		{
			this.start = start;
			return this;
		}

		public Builder withRows(int rows)
		{
			this.rows = rows;
			return this;
		}

		public <T> Builder withOption(SearchOption<T> option, T value)
		{
			options.put(option, value);
			return this;
		}

		public Builder filter(String query)
		{
			this.filterQuery = query;
			return this;
		}

		public Builder sort(String field)
		{
			this.sortField = field;
			return this;
		}

		public Builder returnFields(Iterable<String> fields)
		{
			this.returnFields = new ArrayList<String>();
			for (String field : fields)
			{
				returnFields.add(field);
			}
			return this;
		}

		public Builder returnFields(String... fields)
		{
			this.returnFields(Arrays.asList(fields));
			return this;
		}

		public Search build()
		{
			return new Search(this);
		}

	}

}
