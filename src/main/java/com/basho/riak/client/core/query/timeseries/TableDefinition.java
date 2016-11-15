package com.basho.riak.client.core.query.timeseries;

import java.util.*;

/**
 * Holds a definition for a Table in Time Series Riak.
 * Immutable once created.
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.4
 */
public class TableDefinition
{
    private final String tableName;
    private final LinkedHashMap<String, FullColumnDescription> fullColumnDescriptions = new LinkedHashMap<>();
    private transient List<FullColumnDescription> partitionKeys;
    private transient List<FullColumnDescription> localKeys;
    private transient FullColumnDescription quantumField;

    /**
     * Create a new Table Definition
     * @param tableName a Table Name, required.
     * @param fullColumnDescriptions a list of FullColumnDescription that represents the columns in the table.
     */
    public TableDefinition(String tableName, Iterable<FullColumnDescription> fullColumnDescriptions)
    {
        checkObjectInput(tableName, fullColumnDescriptions);

        this.tableName = tableName;

        for (FullColumnDescription col : fullColumnDescriptions)
        {
            this.fullColumnDescriptions.put(col.getName(), col);
        }
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
    public Collection<FullColumnDescription> getFullColumnDescriptions()
    {
        return fullColumnDescriptions.values();
    }

    /**
     * Look up a FullColumnDescription in this TableDefinition by it's column name.
     * @param columnName the column name to look up.
     * @return the matching FullColumnDescription. Returns <b>null</b> if no column with that name exists.
     */
    public FullColumnDescription getDescriptionByColumnName(String columnName)
    {
        return this.fullColumnDescriptions.get(columnName);
    }

    /**
     * Get the collection of Partition Key FullColumnDescriptions, in order.
     * @return an iterable collection.
     */
    public Collection<FullColumnDescription> getPartitionKeyColumnDescriptions()
    {
        if (partitionKeys == null)
        {
            partitionKeys = new LinkedList<>();

            for (FullColumnDescription col : fullColumnDescriptions.values())
            {
                if (col.isPartitionKeyMember())
                {
                    this.partitionKeys.add(col);
                }
            }

            Collections.sort(this.partitionKeys, PartitionKeyComparator.INSTANCE);
        }

        return this.partitionKeys;
    }

    /**
     * Get the FullColumnDescription for a quantum field, if any.
     * @return null if there is no quantum information
     */
    public FullColumnDescription getQuantumDescription()
    {
        if (quantumField == null)
        {
            for (FullColumnDescription fd: getPartitionKeyColumnDescriptions())
            {
                if (fd.hasQuantum())
                {
                    if (quantumField != null)
                    {
                        throw new IllegalStateException("Table definition has more than one quantum.");
                    }
                    else
                    {
                        quantumField = fd;
                    }
                }
            }
        }

        return quantumField;
    }

    /**
     * Get the collection of Local Key FullColumnDescriptions, in order.
     * @return an iterable collection.
     */
    public Collection<FullColumnDescription> getLocalKeyColumnDescriptions()
    {
        if (localKeys == null)
        {
            localKeys = new LinkedList<>();

            for (FullColumnDescription col : fullColumnDescriptions.values())
            {
                if (col.isLocalKeyMember())
                {
                    this.localKeys.add(col);
                }
            }

            Collections.sort(this.localKeys, LocalKeyComparator.INSTANCE);
        }

        return this.localKeys;
    }

    private void checkObjectInput(String tableName, Iterable<FullColumnDescription> fullColumnDescriptions)
    {
        if (tableName == null || tableName.isEmpty())
        {
            throw new IllegalArgumentException("tableName must not be null or empty.");
        }

        if (fullColumnDescriptions == null || !fullColumnDescriptions.iterator().hasNext())
        {
            throw new IllegalArgumentException("fullColumnDescriptions must not be null or empty.");
        }
    }

    /**
     * Compare LocalKeys for sorting. If there are any non-key columns they will be placed at the end.
     */
    private static class LocalKeyComparator implements Comparator<FullColumnDescription>
    {
        static final LocalKeyComparator INSTANCE = new LocalKeyComparator();
        @Override
        public int compare(FullColumnDescription o1, FullColumnDescription o2)
        {
            if (!o1.isLocalKeyMember() && !o2.isLocalKeyMember())
            {
                return 0;
            }
            else if (!o1.isLocalKeyMember() && o2.isLocalKeyMember())
            {
                return 1;
            }
            else if (o1.isLocalKeyMember() && !o2.isLocalKeyMember())
            {
                return -1;
            }
            else
            {
                return o1.getLocalKeyOrdinal() - o2.getLocalKeyOrdinal();
            }
        }
    }

    /**
     * Compare LocalKeys for sorting. If there are any non-key columns they will be placed at the end.
     */
    private static class PartitionKeyComparator implements Comparator<FullColumnDescription>
    {
        static final LocalKeyComparator INSTANCE = new LocalKeyComparator();
        @Override
        public int compare(FullColumnDescription o1, FullColumnDescription o2)
        {
            if (!o1.isPartitionKeyMember() && !o2.isPartitionKeyMember())
            {
                return 0;
            }
            else if (!o1.isPartitionKeyMember() && o2.isPartitionKeyMember())
            {
                return 1;
            }
            else if (o1.isPartitionKeyMember() && !o2.isPartitionKeyMember())
            {
                return -1;
            }
            else
            {
                return o1.getPartitionKeyOrdinal() - o2.getPartitionKeyOrdinal();
            }
        }
    }
}

