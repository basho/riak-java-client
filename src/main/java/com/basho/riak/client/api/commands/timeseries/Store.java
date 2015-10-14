package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesStoreOperation;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.query.timeseries.TableDefinition;
import com.basho.riak.client.core.query.timeseries.TimeSeriesValidator;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by alex on 8/24/15.
 */
public class Store extends RiakCommand<Void,BinaryValue>
{
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Builder builder;

    private Store (Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<Void, BinaryValue> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, BinaryValue> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private TimeSeriesStoreOperation buildCoreOperation()
    {
        return new TimeSeriesStoreOperation.Builder(BinaryValue.create(builder.tableName.unsafeGetValue()))
                .withRows(builder.rows)
                .build();
    }

    public enum ValidationType
    {
        NONE,
        FIRST_ROW,
        ALL
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final List<Row> rows = new LinkedList<Row>();
        private final TableDefinition tableDefinition;
        private final ValidationType validationType;

        public Builder(BinaryValue tableName)
        {
            this.tableName = tableName;
            this.tableDefinition = null;
            this.validationType = ValidationType.NONE;
        }

        public Builder(String tableName)
        {
            this(BinaryValue.createFromUtf8(tableName));
        }

        public Builder(TableDefinition tableDefinition, ValidationType validationType)
        {
            this.tableName = BinaryValue.createFromUtf8(tableDefinition.getTableName());
            this.tableDefinition = tableDefinition;
            this.validationType = validationType;
        }

        public Builder withRow(Row row)
        {
            this.rows.add(row);
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.rows.addAll(rows);
            return this;
        }

        public Store build()
        {
            TimeSeriesValidator.ValidationResult validationResult = null;
            switch (this.validationType) {
                case FIRST_ROW :
                     validationResult = TimeSeriesValidator.validate(tableDefinition, rows.get(0));
                    break;
                case ALL:
                    validationResult = TimeSeriesValidator.validateAll(tableDefinition, rows);
                    break;
            }

            if(validationResult != null && validationResult.isSuccess() == false)
            {
                throw new IllegalArgumentException(validationResult.getErrorMessage());
            }


            return new Store(this);
        }
    }
}
