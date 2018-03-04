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
package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.*;

/**
 * Command used to perform a search in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * To do a search in Riak, you must have search turned on, indexes created, and documents/objects indexed.
 * Please see http://docs.basho.com/riak/latest/dev/using/search/ for more information.
 *
 * To use the Search class, you need a minimum of an Index to search, and a query to execute.
 * Other options are available for pagination, sorting, filtering and what fields to return.
 *
 * <pre class="prettyprint">
 * {@code
 * String index = "Author_Biographies";
 * String query = "name_s:Al* AND bio_tsd:awesome";
 *
 * Search sch =
 *      new Search.Builder(index, query).withRows(10).build();
 * SearchOperation.Response response = client.execute(sch);}</pre>
 * </p>
 * <p>
 * All operations can called async as well.
 * <pre class="prettyprint">
 * {@code
 * ...
 * RiakFuture<SearchOperation.Response, BinaryValue> future = client.executeAsync(sv);
 * ...
 * future.await();
 * if (future.isSuccess())
 * {
 *     ...
 * }}</pre>
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class Search extends AsIsRiakCommand<SearchOperation.Response, BinaryValue>
{
    /**
     * Enum that encapsulates the possible settings for a search command's presort setting.
     * Presort results by the key, or search score.
     */
    public enum Presort
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
    private final Map<Option<?>, Object> options = new HashMap<>();

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
    protected SearchOperation buildCoreOperation()
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

        if (rows >= 0)
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (!(o instanceof Search))
        {
            return false;
        }

        Search search = (Search) o;

        return start == search.start &&
                rows == search.rows &&
                Objects.equals(index, search.index) &&
                Objects.equals(query, search.query) &&
                presort == search.presort &&
                Objects.equals(filterQuery, search.filterQuery) &&
                Objects.equals(sortField, search.sortField) &&
                Objects.equals(returnFields, search.returnFields) &&
                Objects.equals(options, search.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index, query, start, rows, presort, filterQuery, sortField, returnFields, options);
    }

    /*
		* Options For controlling how Riak performs the search operation.
		* <p>
		* These options can be supplied to the {@link Search.Builder} to change
		* how Riak performs the operation. These override the defaults provided
		* by Riak.
		* </p>
		* @author Dave Rusek <drusek at basho dot com>
		* @since 2.0
		*/
   public static final class Option<T> extends RiakOption<T>
   {
       /**
        * Enum that encapsulates the possible operators for a Riak search query default operator field.
        * Can be an "AND", or "OR" operator.
        */
       public static enum Operation
       {
           AND("and"), OR("or");

           final String opStr;

           Operation(String opStr)
           {
               this.opStr = opStr;
           }
       }

       public static Option<Operation> DEFAULT_OPERATION = new Option<>("DEFAULT_OPERATION");
       public static Option<String> DEFAULT_FIELD = new Option<>("DEFAULT_FIELD");

       private Option(String name)
       {
           super(name);
       }
   }

   /**
    * Builder for a Search command.
    */
    public static class Builder
    {
        private final String index;
        private final String query;
        private int start = -1;
        private int rows = -1;
        private Presort presort;
        private String filterQuery;
        private String sortField;
        private List<String> returnFields;
        private Map<Option<?>, Object> options = new HashMap<>();

        /**
         * Construct a Builder for a Search command.
         * @param index The index to search.
         * @param query The query to execute against the index.
         */
        public Builder(String index, String query)
        {
            this.index = index;
            this.query = query;
        }

        /**
         * Set the presort option, you may presort the results by Key or Score.
         * @param presort the {@link com.basho.riak.client.api.commands.search.Search.Presort} option to set.
         * @return a reference to this object.
         */
        public Builder withPresort(Presort presort)
        {
            this.presort = presort;
            return this;
        }

        /**
         * Set the starting row to return.
         * @param start the first row to return out of the result set.
         *              Use in conjunction with {@link #withRows(int)} to paginate the results.
         * @return a reference to this object.
         */
        public Builder withStart(int start)
        {
            this.start = start;
            return this;
        }

        /**
         * Set the maximum number of rows to return.
         * @param rows The total number of rows to return.
         *             Use in conjunction with {@link #withStart(int)} to paginate the results.
         * @return a reference to this object.
         */
        public Builder withRows(int rows)
        {
            this.rows = rows;
            return this;
        }

        /**
         * Add an optional setting for this command.
         * This will be passed along with the request to Riak to tell it how
         * to behave when servicing the request.
         *
         * @param option the option.
         * @param value the value for the option.
         * @return a reference to this object.
         */
        public <T> Builder withOption(Option<T> option, T value)
        {
            options.put(option, value);
            return this;
        }

        /**
         * Set a filter to use for this search.
         * @param query the query string to filter the search with.
         * @return a reference to this object.
         */
        public Builder filter(String query)
        {
            this.filterQuery = query;
            return this;
        }

        /**
         * Set a field to sort the results on.
         * @param field the field to sort the results with.
         * @return a reference to this object.
         */
        public Builder sort(String field)
        {
            this.sortField = field;
            return this;
        }

        /**
         * Set the list of fields that should be returned for each record in the result set.
         * @param fields the collection of fields to return with each result.
         * @return a reference to this object.
         */
        public Builder returnFields(Iterable<String> fields)
        {
            this.returnFields = new ArrayList<>();
            for (String field : fields)
            {
                returnFields.add(field);
            }
            return this;
        }

        /**
         * Set the list of fields that should be returned for each record in the result set.
         * @param fields the varargs list of fields to return with each result.
         * @return a reference to this object.
         */
        public Builder returnFields(String... fields)
        {
            this.returnFields(Arrays.asList(fields));
            return this;
        }

        /**
         * Construct the Search command.
         * @return the new Search command.
         */
        public Search build()
        {
            return new Search(this);
        }
    }
}
