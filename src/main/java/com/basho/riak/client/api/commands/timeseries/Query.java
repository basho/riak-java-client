package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.ts.QueryOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Time Series Query Command
 * Allows you to query a Time Series table, with the given query string.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Query extends AsIsRiakCommand<QueryResult, String>
{
    private final Builder builder;

    private Query(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected QueryOperation buildCoreOperation()
    {
        return new QueryOperation.Builder(builder.queryText)
                                           .withCoverageContext(builder.coverageContext)
                                           .withQueryBufferReuse(builder.queryBufferReuse)
                                           .build();
    }

    /**
     * Used to construct a Time Series Query command.
     */
    public static class Builder
    {
        private static final Logger logger = LoggerFactory.getLogger(Query.Builder.class);
        private static final Pattern paramPattern = Pattern.compile("(:[a-zA-Z][0-9a-zA-Z_]*)");

        private final String queryText;
        private final Map<String, BinaryValue> interpolations = new HashMap<>();
        private final Set<String> knownParams;
        private byte[] coverageContext = null;
        private boolean queryBufferReuse = false;

        /**
         * Construct a Builder for a Time Series Query command.
         * @param queryText Required. The query to run.
         */
        public Builder(String queryText)
        {
            if (queryText == null || queryText.isEmpty())
            {
                String msg = "Query Text must not be null or empty";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }

            this.queryText = queryText;

            Matcher paramMatcher = paramPattern.matcher(queryText);

            if (!paramMatcher.matches())
            {
                knownParams = Collections.emptySet();
                return;
            }

            knownParams = new HashSet<>(paramMatcher.groupCount());

            for (int i = 0; i < paramMatcher.groupCount(); i++)
            {
                knownParams.add(paramMatcher.group(i));
            }
        }

        public Builder(String queryText, byte[] coverageContext)
        {
            this(queryText);
            this.coverageContext = coverageContext;
        }

        private Builder addParameter(String keyString, String key, BinaryValue value)
        {
            checkParamValidity(keyString);
            interpolations.put(key, value);
            return this;
        }

        public Builder withCoverageContext(byte[] coverageContext)
        {
            this.coverageContext = coverageContext;
            return this;
        }

        private void checkParamValidity(String paramName)
        {
            if (!knownParams.contains(paramName))
            {
                String msg = "Unknown query parameter: " + paramName;
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        /**
         * Add the option for Riak to cache the temp table used for the sorting and paging of a query.
         * <p>
         *     If enabled, this can make subsequent result page fetches faster, but at the cost of
         *     possibly stale information. Updates that occur before the cache expires will not be
         *     seen in the temp table.
         * </p>
         * <p>
         *     This can only be used for queries that use either the LIMIT, OFFSET, or ORDER BY keywords,
         *     and the setting will be ignored in other queries.</p>
         * <p>
         *     The temp table used to sort and page results will expire and be deleted after a set amount of time.
         *     Please see Riak's <q>riak_kv.timeseries_query_buffers_expire_ms</q>
         *     and <q>riak_kv.timeseries_query_buffers_incomplete_release_ms</q> settings for more
         *     information on expiry time.
         * </p>
         * <p>
         *     <b>NOTE: </b>
         *     This caching will only work if subsequent queries are sent to the
         *     same node that the first query was sent to.
         *     It is incompatible with Load Balancers at this time.
         * </p>
         * Default is false.
         * @param queryBufferReuse Whether to cache the temp table or not.
         * @return a reference to this object
         */
        public Builder withTempTableCaching(boolean queryBufferReuse)
        {
            this.queryBufferReuse = queryBufferReuse;
            return this;
        }

        /**
         * Construct a Time Series Query object.
         * @return a new Time Series Query instance.
         */
        public Query build()
        {
            return new Query(this);
        }
    }
}
