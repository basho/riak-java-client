package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class CollectionConverters
{
    private CollectionConverters() {}

    public static Collection<RiakTsPB.TsColumnDescription> convertColumnDescriptionsToPb
            (Collection<ColumnDescription> columns)
    {
        final ArrayList<RiakTsPB.TsColumnDescription> pbColumns =
                new ArrayList<RiakTsPB.TsColumnDescription>(columns.size());

        for (ColumnDescription column : columns)
        {
            pbColumns.add(convertColumnDescriptionToPb(column));
        }

        return pbColumns;
    }

    private static RiakTsPB.TsColumnDescription convertColumnDescriptionToPb(ColumnDescription column)
    {
        final RiakTsPB.TsColumnDescription.Builder columnBuilder = RiakTsPB.TsColumnDescription.newBuilder();
        columnBuilder.setName(ByteString.copyFromUtf8(column.getName()));

        columnBuilder.setType(RiakTsPB.TsColumnType.valueOf(column.getType().ordinal()));

        return columnBuilder.build();
    }

    public static List<ColumnDescription> convertPBColumnDescriptions(List<RiakTsPB.TsColumnDescription> pbColumns)
    {
        if (pbColumns == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<ColumnDescription> columns = new ArrayList<ColumnDescription>(pbColumns.size());

        for (RiakTsPB.TsColumnDescription pbColumn : pbColumns)
        {
            ColumnDescription columnDescription = convertPBColumnDescription(pbColumn);
            columns.add(columnDescription);
        }

        return columns;
    }

    private static ColumnDescription convertPBColumnDescription(RiakTsPB.TsColumnDescription pbColumn)
    {
        final String name = pbColumn.getName().toStringUtf8();

        final ColumnDescription.ColumnType type = ColumnDescription.ColumnType.values()[pbColumn.getType().getNumber()];

        return new ColumnDescription(name, type);
    }

    public static List<FullColumnDescription> convertDescribeQueryResultToColumnDescriptions(QueryResult queryResult)
    {
        final List<FullColumnDescription> fullColumnDescriptions = new ArrayList<>(queryResult.getRowsCount());

        for (Row row : queryResult)
        {
            fullColumnDescriptions.add(convertDescribeResultRowToFullColumnDescription(row));
        }

        return fullColumnDescriptions;
    }

    private static FullColumnDescription convertDescribeResultRowToFullColumnDescription(Row row)
    {
        /*
         * Expected Format for the DESCRIBE function is 5 or 7 columns depending on the version.
         *
         *  V1 includes:
         *   "Column"        (non-null Varchar)
         *   "Type"          (non-null Varchar)
         *   "Is Null"       (non-null Boolean)
         *   "Partition Key" (nullable SInt64)
         *   "Local Key"     (nullable SInt64)
         *
         *  V2 also includes:
         *   "Interval", part of the quantum information (nullable SInt64)
         *   "Unit", part of the quantum information     (nullable Varchar), either 'd', 'h', 'm', or 's'
         */

        final List<Cell> cells = row.getCellsCopy();

        assert(DescribeFnRowResultIsValid(cells));

        final String name = cells.get(0).getVarcharAsUTF8String();
        final String typeString = cells.get(1).getVarcharAsUTF8String();
        final boolean isNullable = cells.get(2).getBoolean();
        final boolean isPartitionKeyMember = cells.get(3) != null;
        final boolean isLocalKeyMember = cells.get(4) != null;
        final Integer partitionKeyOrdinal = isPartitionKeyMember ? new Long(cells.get(3).getLong()).intValue() : null;
        final Integer localKeyOrdinal = isLocalKeyMember ? new Long(cells.get(4).getLong()).intValue() : null;

        final ColumnDescription.ColumnType type =
                ColumnDescription.ColumnType.valueOf(typeString.toUpperCase(Locale.ENGLISH));

        final Quantum quantum = parseQuantumCells(cells);

        return new FullColumnDescription(name,
                                         type,
                                         isNullable,
                                         partitionKeyOrdinal,
                                         localKeyOrdinal,
                                         quantum);
    }

    private static Quantum parseQuantumCells(List<Cell> cells)
    {
        final boolean hasQuantum = cells.size() > 5 && cells.get(5) != null && cells.get(6) != null;

        final Long quantumInterval = hasQuantum ? cells.get(5).getLong() : null;
        final TimeUnit quantumUnit = hasQuantum ? Quantum.parseTimeUnit(cells.get(6).getVarcharAsUTF8String()) : null;

        return hasQuantum ?
            new Quantum(quantumInterval.intValue(), quantumUnit) :
            null;
    }

    private static boolean DescribeFnRowResultIsValid(List<Cell> cells)
    {
        final boolean isValidV1Description = cells.size() == 5 && describeRowBaseIsValid(cells);

        final boolean isValidV2Description =
                cells.size() == 7 &&
                describeRowBaseIsValid(cells) &&
                cells.get(5) != null ? cells.get(5).hasLong() : true &&
                cells.get(6) != null ? cells.get(6).hasVarcharValue() : true;

        return isValidV1Description || isValidV2Description;
    }

    private static boolean describeRowBaseIsValid(List<Cell> cells)
    {
        return cells.size() >= 5 &&
               cells.get(0).hasVarcharValue() &&
               cells.get(1).hasVarcharValue() &&
               cells.get(2).hasBoolean() &&
               cells.get(3) != null ? cells.get(3).hasLong() : true &&
               cells.get(4) != null ? cells.get(4).hasLong() : true;
    }
}
