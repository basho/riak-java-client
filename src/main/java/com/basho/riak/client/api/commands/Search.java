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
package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.*;

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
    private final Map<Option<?>, Object> options =
	    new HashMap<Option<?>, Object>();

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
    protected RiakFuture<SearchOperation.Response, BinaryValue> executeAsync(RiakCluster cluster)
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
                protected BinaryValue convertQueryInfo(BinaryValue coreQueryInfo)
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

        for (Map.Entry<Option<?>, Object> option : options.entrySet())
        {

            if (option.getKey() == Option.DEFAULT_FIELD)
            {
                builder.withDefaultField((String) option.getValue());
            }
            else if (option.getKey() == Option.DEFAULT_OPERATION)
            {
                Option.Operation op = (Option.Operation) option.getValue();
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

    /*
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    */
   public static final class Option<T> extends RiakOption<T>
   {

       public static enum Operation
       {
           AND("and"), OR("or");

           final String opStr;

           Operation(String opStr)
           {
               this.opStr = opStr;
           }
       }

       public static Option<Operation> DEFAULT_OPERATION = new Option<Operation>("DEFAULT_OPERATION");
       public static Option<String> DEFAULT_FIELD = new Option<String>("DEFAULT_FIELD");

       private Option(String name)
       {
           super(name);
       }
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
		private Map<Option<?>, Object> options = new HashMap<Option<?>, Object>();

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

		public <T> Builder withOption(Option<T> option, T value)
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
