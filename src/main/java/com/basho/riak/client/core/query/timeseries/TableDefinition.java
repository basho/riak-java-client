package com.basho.riak.client.core.query.timeseries;

import java.util.List;

/**
 * Holds a definition for a Table in Time Series Riak.
 * Immutable once created.
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.4
 */
public class TableDefinition
{
    private final String tableName;
    private final List<FullColumnDescription> fullColumnDescriptions;

    public TableDefinition(String tableName, List<FullColumnDescription> fullColumnDescriptions)
    {
        this.tableName = tableName;
        this.fullColumnDescriptions = fullColumnDescriptions;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<FullColumnDescription> getFullColumnDescriptions()
    {
        return fullColumnDescriptions;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        TableDefinition that = (TableDefinition) o;

        if (!tableName.equals(that.tableName))
        {
            return false;
        }
        return fullColumnDescriptions.equals(that.fullColumnDescriptions);

    }

    @Override
    public int hashCode()
    {
        int result = tableName.hashCode();
        result = 31 * result + fullColumnDescriptions.hashCode();
        return result;
    }
}

