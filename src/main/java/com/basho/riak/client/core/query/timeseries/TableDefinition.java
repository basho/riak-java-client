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

    /**
     * Create a new Table Definition
     * @param tableName a Table Name, required.
     * @param fullColumnDescriptions a list of FullColumnDescription that represents the columns in the table.
     */
    public TableDefinition(String tableName, List<FullColumnDescription> fullColumnDescriptions)
    {
        if(tableName == null || tableName.isEmpty())
        {
            throw new IllegalArgumentException("Table Name must not be null or empty.");
        }

        if(fullColumnDescriptions == null || fullColumnDescriptions.isEmpty())
        {
            throw new IllegalArgumentException("Full Column Descriptions List must not be null or empty.");
        }

        this.tableName = tableName;
        this.fullColumnDescriptions = fullColumnDescriptions;
    }

    /**
     * Get the table name for this definition.
     * @return a String table name.
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Get the ordered list of full column descriptions for this definition.
     * @return a List of the FullColumnDescription in this definition.
     */
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

