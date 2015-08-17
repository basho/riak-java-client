package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by alex on 8/14/15.
 */
public class Query extends RiakCommand<QueryResult, BinaryValue>
{
    private final Builder builder;
    private final Logger logger = LoggerFactory.getLogger(Query.class);
    private Query(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<QueryResult, BinaryValue> executeAsync(RiakCluster cluster) {
        RiakFuture<QueryResult, BinaryValue> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private TimeSeriesQueryOperation buildCoreOperation()
    {
        return new TimeSeriesQueryOperation.Builder(builder.queryText)
                                           .setInterpolations(builder.interpolations)
                                           .build();
    }

    public static class Builder
    {
        private final Logger logger = LoggerFactory.getLogger(Query.Builder.class);

        // TODO: Double check valid param syntax
        private final Pattern paramPattern = Pattern.compile(":([a-zA-Z][-\\w]*+):");
        private final BinaryValue queryText;
        private final Map<BinaryValue, BinaryValue> interpolations = new HashMap<BinaryValue, BinaryValue>();
        private final HashSet<String> knownParams = new HashSet<String>();

        public Builder(String queryText)
        {
            if(queryText == null || queryText.isEmpty())
            {
                String msg = "Query Text must not be null or empty";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }


            this.queryText = BinaryValue.createFromUtf8(queryText);

            Matcher paramMatcher = paramPattern.matcher(queryText);

            if(!paramMatcher.matches()) { return; }

            for (int i = 0; i < paramMatcher.groupCount(); i++) {
                knownParams.add(paramMatcher.group(i));
            }
        }

        public Builder addStringParameter(String key, String value)
        {
            checkParamValidity(key);
            return this.addParameter(BinaryValue.createFromUtf8(key), BinaryValue.createFromUtf8(value));
        }

        public Builder addStringParameters(Map<String, String> parameters)
        {
            for( Map.Entry<String, String> parameter : parameters.entrySet())
            {
                addStringParameter(parameter.getKey(), parameter.getValue());
            }
            return this;
        }

        private void checkParamValidity(String paramName)
        {
            if(!knownParams.contains(paramName))
            {
                String msg = "Unknown query parameter: " + paramName;
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        private Builder addParameter(BinaryValue key, BinaryValue value)
        {
            interpolations.put(key, value);
            return this;
        }

        public Query build()
        {
            return new Query(this);
        }

    }
}
