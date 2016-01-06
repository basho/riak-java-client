package com.basho.riak.client.core.query.timeseries;

/**
 * A Metadata description of a column in Riak Time Series.
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

    public ColumnDescription(String name, ColumnType type)
    {
        if(name == null || name.isEmpty())
        {
            throw new IllegalArgumentException("Column Name must not be null or empty.");
        }

        if(type == null)
        {
            throw new IllegalArgumentException("Column Type must not be null.");
        }

        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

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
        BOOLEAN
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
