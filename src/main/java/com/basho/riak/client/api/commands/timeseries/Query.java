package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.QueryOperation;
import com.basho.riak.client.core.query.timeseries.IQueryResult;
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
public class Query extends RiakCommand<IQueryResult, BinaryValue>
{
    private static final Logger logger = LoggerFactory.getLogger(Query.class);

    private final Builder builder;

    private Query(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<IQueryResult, BinaryValue> executeAsync(RiakCluster cluster) {
        return cluster.execute(buildCoreOperation());
    }

    private QueryOperation buildCoreOperation()
    {
        return new QueryOperation.Builder(builder.queryText)
                                           //.setInterpolations(builder.interpolations)
                                           .build();
    }

    public static class Builder
    {
        private static final Logger logger = LoggerFactory.getLogger(Query.Builder.class);
        private static final Pattern paramPattern = Pattern.compile("(:[a-zA-Z][0-9a-zA-Z_]*)");

        private final BinaryValue queryText;
        private final Map<BinaryValue, BinaryValue> interpolations = new HashMap<BinaryValue, BinaryValue>();
        private final Set<String> knownParams;

        public Builder(String queryText)
        {
            if (queryText == null || queryText.isEmpty())
            {
                String msg = "Query Text must not be null or empty";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }

            this.queryText = BinaryValue.createFromUtf8(queryText);

            Matcher paramMatcher = paramPattern.matcher(queryText);

            if (!paramMatcher.matches())
            {
                knownParams = Collections.emptySet();
                return;
            }

            knownParams = new HashSet<String>(paramMatcher.groupCount());

            for (int i = 0; i < paramMatcher.groupCount(); i++) {
                knownParams.add(paramMatcher.group(i));
            }
        }

        private Builder addParameter(String keyString, BinaryValue key, BinaryValue value)
        {
            checkParamValidity(keyString);
            interpolations.put(key, value);
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

        public Query build()
        {
            return new Query(this);
        }

    }
}
