package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.*;

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
        final List<Cell> cells = row.getCellsCopy();

        final String name = cells.get(0).getVarcharAsUTF8String();
        final String typeString = cells.get(1).getVarcharAsUTF8String();
        final boolean isNullable = cells.get(2).getBoolean();
        final boolean isPrimaryKeyMember = cells.get(3) != null;
        final boolean isLocalKeyMember = cells.get(4) != null;
        final Integer primaryKeyOrdinal = isPrimaryKeyMember ? new Long(cells.get(3).getLong()).intValue() : null;
        final Integer localKeyOrdinal = isLocalKeyMember ? new Long(cells.get(4).getLong()).intValue() : null;

        final ColumnDescription.ColumnType type =
                ColumnDescription.ColumnType.valueOf(typeString.toUpperCase(Locale.ENGLISH));

        return new FullColumnDescription(name,
                                         type,
                                         isNullable,
                                         primaryKeyOrdinal,
                                         localKeyOrdinal);
    }
}
