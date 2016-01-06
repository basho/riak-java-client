package com.basho.riak.client.core.query.timeseries;

/**
 * Holds a complete definition for a Table Column in Time Series Riak.
 * Immutable once created.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.4
 */
public class FullColumnDescription
{
    private final ColumnDescription baseColumnDescription;
    private final boolean isNullable;
    private final Integer primaryKeyOrdinal;
    private final Integer localKeyOrdinal;

    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable)
    {
        this(name, type, isNullable, null, null);
    }

    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable,
                                 Integer keyOrdinal) {
        this(name, type, isNullable, keyOrdinal, keyOrdinal);
    }

    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable,
                                 Integer primaryKeyOrdinal,
                                 Integer localKeyOrdinal)
    {
        this.baseColumnDescription = new ColumnDescription(name, type);
        this.isNullable = isNullable;
        this.primaryKeyOrdinal = primaryKeyOrdinal;
        this.localKeyOrdinal = localKeyOrdinal;
    }

    public String getName()
    {
        return this.baseColumnDescription.getName();
    }

    public ColumnDescription.ColumnType getType()
    {
        return this.baseColumnDescription.getType();
    }

    public boolean isNullable()
    {
        return isNullable;
    }

    public boolean isPartitionKeyMember()
    {
        return primaryKeyOrdinal != null;
    }

    public boolean isLocalKeyMember()
    {
        return localKeyOrdinal != null;
    }

    public Integer getPrimaryKeyOrdinal()
    {
        return primaryKeyOrdinal;
    }

    public Integer getLocalKeyOrdinal()
    {
        return localKeyOrdinal;
    }

}
