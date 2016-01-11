package com.basho.riak.client.core.query.timeseries;

/**
 * Holds a complete definition for a Table Column in Time Series Riak.
 * Immutable once created.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.4
 */
public class FullColumnDescription extends ColumnDescription
{
    private final boolean isNullable;
    private final Integer partitionKeyOrdinal;
    private final Integer localKeyOrdinal;

    /**
     * Creates a basic FullColumnDescription, for non-key columns.
     * @param name The name of the column. Required - must not be null or an empty string.
     * @param type The type of the column. Required - must not be null.
     * @param isNullable The nullability of the column.
     * @throws IllegalArgumentException if Column Name or Column Type are null or empty.
     */
    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable)
    {
        this(name, type, isNullable, null, null);
    }

    /**
     * Creates a FullColumnDescription. Useful for key columns where the partition and local key oridinals are the same.
     * @param name The name of the column. Required - must not be null or an empty string.
     * @param type The type of the column. Required - must not be null.
     * @param isNullable The nullability of the column.
     * @param keyOrdinal The ordinal number of where this column appears in the ordered Local Key column set.
     *                   <b>Use null if not a key column.</b>
     * @throws IllegalArgumentException if Column Name or Column Type are null or empty.
     */
    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable,
                                 Integer keyOrdinal)
    {
        this(name, type, isNullable, keyOrdinal, keyOrdinal);
    }

    /**
     * Creates a FullColumnDescription.
     * Useful for automating creation of FullColumnDescriptions where the values can vary.
     * @param name The name of the column. Required - must not be null or an empty string.
     * @param type The type of the column. Required - must not be null.
     * @param isNullable The nullability of the column.
     * @param partitionKeyOrdinal The ordinal number of where this column appears in
     *                            the ordered Partition Key column set.
     *                            <b>Use null if not a key column.</b>
     * @param localKeyOrdinal The ordinal number of where this column appears in
     *                        the ordered Local Key column set.
     *                        <b>Use null if not a key column.</b>
     * @throws IllegalArgumentException if Column Name or Column Type are null or empty.
     */
    public FullColumnDescription(String name,
                                 ColumnDescription.ColumnType type,
                                 boolean isNullable,
                                 Integer partitionKeyOrdinal,
                                 Integer localKeyOrdinal)
    {
        super(name, type);
        this.isNullable = isNullable;
        this.partitionKeyOrdinal = partitionKeyOrdinal;
        this.localKeyOrdinal = localKeyOrdinal;
    }

    /**
     * Whether this column's values are nullable.
     * @return boolean
     */
    public boolean isNullable()
    {
        return isNullable;
    }

    /**
     * Whether this column is a member of the Partition Key column set.
     * @return boolean.
     */
    public boolean isPartitionKeyMember()
    {
        return partitionKeyOrdinal != null;
    }

    /**
     * Whether this column is a member of the Local Key column set.
     * @return boolean.
     */
    public boolean isLocalKeyMember()
    {
        return localKeyOrdinal != null;
    }

    /**
     * Get the ordinal number of where this column appears in the ordered Partition Key column set.
     * @return Integer if {@link #isPartitionKeyMember()} is <i>true</i>,
     *         <b>null</b> if {@link #isPartitionKeyMember()} is <i>false</i>.
     */
    public Integer getPartitionKeyOrdinal()
    {
        return partitionKeyOrdinal;
    }

    /**
     * Get the ordinal number of where this column appears in the ordered Local Key column set.
     * @return Integer if {@link #isLocalKeyMember()} is <i>true</i>,
     *         <b>null</b> if {@link #isLocalKeyMember()} is <i>false</i>.
     */
    public Integer getLocalKeyOrdinal()
    {
        return localKeyOrdinal;
    }
}
