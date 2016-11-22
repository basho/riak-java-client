package com.basho.riak.client.core.query.timeseries;

/**
 * A minimal metadata description of a column in Riak Time Series.
 * Contains a column name and column type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class ColumnDescription
{
    private final String name;
    private final ColumnType type;

    /**
     * Create a new ColumnDescription.
     * @param name The name of the column. Required - must not be null or an empty string.
     * @param type The type of the column. Required - must not be null.
     * @throws IllegalArgumentException if Column Name or Column Type are null or empty.
     */
    public ColumnDescription(String name, ColumnType type)
    {
        if (name == null || name.isEmpty())
        {
            throw new IllegalArgumentException("Column Name must not be null or empty.");
        }

        if (type == null)
        {
            throw new IllegalArgumentException("Column Type must not be null.");
        }

        this.name = name;
        this.type = type;
    }

    /**
     * Get the Column Name.
     * @return the column name String.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Get the Column Type.
     * @return the ColumnType value.
     */
    public ColumnType getType()
    {
        return type;
    }

    /**
     * Values MUST BE IN THE SAME ORDER AS in the RiakTsPB.TsColumnType
     */
    public enum ColumnType
    {
        VARCHAR,
        SINT64,
        DOUBLE,
        TIMESTAMP,
        BOOLEAN,
        BLOB
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

        ColumnDescription that = (ColumnDescription) o;

        if (!name.equals(that.name))
        {
            return false;
        }
        return type == that.type;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
