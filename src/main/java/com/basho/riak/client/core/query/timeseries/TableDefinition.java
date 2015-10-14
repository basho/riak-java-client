package com.basho.riak.client.core.query.timeseries;

import java.util.Arrays;
import java.util.List;

/**
 *  Describes a Table in a Riak Time Series.
 *  Contains the table name, as well as the layout and types of the columns.
 */
public class TableDefinition
{
    private final String tableName;
    private final List<ColumnDescription> columnDescriptions;

    public TableDefinition(String tableName, ColumnDescription... columnDescriptions)
    {
        this(tableName, Arrays.asList(columnDescriptions));
    }

    public TableDefinition(String tableName, List<ColumnDescription> columnDescriptions)
    {
        this.tableName = tableName;
        this.columnDescriptions = columnDescriptions;
    }

    /**
     * Get the table name.
     * @return The table name.
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Get the column descriptions.
     * @return The column descriptions.
     */
    public List<ColumnDescription> getColumnDescriptions()
    {
        return columnDescriptions;
    }
}
