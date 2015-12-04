package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.timeseries.*;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.*;

/**
 * Converts Time Series Protobuff message types to Java Client entity objects, and back.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class TimeSeriesPBConverter
{
    private TimeSeriesPBConverter() {}

    public static IQueryResult convertPbGetResp(RiakTsPB.TsQueryResp response)
    {
        if (response == null)
        {
            return QueryResult.EMPTY;
        }

        final List<IColumnDescription> columnDescriptions = convertPBColumnDescriptions(response.getColumnsList());
        final List<IRow> rows = convertPbRows(response.getRowsList(), columnDescriptions);

        return new QueryResult(columnDescriptions, rows);
    }

    public static IQueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        if (response == null)
        {
            return QueryResult.EMPTY;
        }

        final List<IColumnDescription> columnDescriptions = convertPBColumnDescriptions(response.getColumnsList());
        final List<IRow> rows = convertPbRows(response.getRowsList(), columnDescriptions);

        return new QueryResult(columnDescriptions, rows);
    }
    public static Collection<RiakTsPB.TsColumnDescription> convertColumnDescriptionsToPb(
            Collection<ColumnDescription> columns)
    {
        final ArrayList<RiakTsPB.TsColumnDescription> pbColumns = new ArrayList<RiakTsPB.TsColumnDescription>(columns.size());

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

    public static Collection<RiakTsPB.TsRow> convertRowsToPb(Collection<Row> rows)
    {
        final ArrayList<RiakTsPB.TsRow> pbRows = new ArrayList<RiakTsPB.TsRow>(rows.size());
        final RiakTsPB.TsRow.Builder rowBuilder = RiakTsPB.TsRow.newBuilder();

        final RiakTsPB.TsCell.Builder cellBuilder = RiakTsPB.TsCell.newBuilder();
        for (Row row : rows)
        {
            final RiakTsPB.TsCell pbCells[] = new RiakTsPB.TsCell[row.getCells().size()];
            int idx = 0;
            for (ICell cell : row.getCells())
            {
                pbCells[idx++] = buildPBCell(cellBuilder, cell).build();
            }

            rowBuilder.addAllCells(Arrays.asList(pbCells));
            pbRows.add(rowBuilder.build());
            rowBuilder.clear();
        }
        return pbRows;
    }

    public static List<RiakTsPB.TsCell> convertCellsToPb(Collection<Cell> cells)
    {
        return convertCellsToPb(RiakTsPB.TsCell.newBuilder(), cells);
    }

    private static List<RiakTsPB.TsCell> convertCellsToPb(RiakTsPB.TsCell.Builder cellBuilder, Collection<Cell> cells)
    {
        final ArrayList<RiakTsPB.TsCell> pbCells = new ArrayList<RiakTsPB.TsCell>(cells.size());

        for (Cell cell : cells)
        {
            pbCells.add(buildPBCell(cellBuilder, cell).build());
        }

        return pbCells;
    }

    private static List<IRow> convertPbRows(List<RiakTsPB.TsRow> pbRows, List<IColumnDescription> columnDescriptions)
    {
        if (pbRows == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<IRow> rows = new ArrayList<IRow>(pbRows.size());

        for (RiakTsPB.TsRow pbRow : pbRows)
        {
            final int numCells = pbRow.getCellsCount();
            final List<ICell> cells = new ArrayList<ICell>(numCells);

            for (int i = 0; i < numCells; ++i)
            {
                final ColumnDescription.ColumnType columnType = columnDescriptions.get(i).getType();
                final RiakTsPB.TsCell pbCell = pbRow.getCells(i);
                cells.add(convertPbCell(pbCell, columnType));
            }

            rows.add(new Row(cells));
        }

        return rows;
    }

    private static Cell convertPbCell(RiakTsPB.TsCell pbCell, ColumnDescription.ColumnType columnType)
    {
        Cell cell;

        if (columnType == ColumnDescription.ColumnType.VARCHAR && pbCell.hasVarcharValue())
        {
            cell = new Cell(BinaryValue.unsafeCreate(pbCell.getVarcharValue().toByteArray()));
        }
        else if (columnType == ColumnDescription.ColumnType.BOOLEAN && pbCell.hasBooleanValue())
        {
            cell = new Cell(pbCell.getBooleanValue());
        }
        else if (columnType == ColumnDescription.ColumnType.SINT64 && pbCell.hasSint64Value())
        {
            cell = new Cell(pbCell.getSint64Value());
        }
        else if (columnType == ColumnDescription.ColumnType.TIMESTAMP && pbCell.hasTimestampValue())
        {
            cell = Cell.newTimestamp(pbCell.getTimestampValue());
        }
        else if (columnType == ColumnDescription.ColumnType.DOUBLE && pbCell.hasDoubleValue())
        {
            cell = new Cell(pbCell.getDoubleValue());
        }
        else // Null cell
        {
            cell = null;
        }

        return cell;
    }

    public static List<IColumnDescription> convertPBColumnDescriptions(List<RiakTsPB.TsColumnDescription> pbColumns)
    {
        if (pbColumns == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<IColumnDescription> columns = new ArrayList<IColumnDescription>(pbColumns.size());

        for (RiakTsPB.TsColumnDescription pbColumn : pbColumns)
        {

            IColumnDescription columnDescription = convertPBColumnDescription(pbColumn);
            columns.add(columnDescription);
        }

        return columns;
    }

    private static IColumnDescription convertPBColumnDescription(RiakTsPB.TsColumnDescription pbColumn)
    {
        final String name = pbColumn.getName().toStringUtf8();

        final ColumnDescription.ColumnType type = ColumnDescription.ColumnType.values()[pbColumn.getType().getNumber()];

        return new ColumnDescription(name, type);
    }

    private static RiakTsPB.TsCell.Builder buildPBCell(RiakTsPB.TsCell.Builder cellBuilder, ICell cell) {
        cellBuilder.clear();

        if (cell == null) {
            return cellBuilder;
        }
        else if (cell.hasVarcharValue())
        {
            cellBuilder.setVarcharValue(ByteString.copyFrom(cell.getVarcharValue().unsafeGetValue()));
        }
        else if (cell.hasBoolean())
        {
            cellBuilder.setBooleanValue(cell.getBoolean());
        }
        else if (cell.hasLong())
        {
            cellBuilder.setSint64Value(cell.getLong());
        }
        else if (cell.hasTimestamp())
        {
            cellBuilder.setTimestampValue(cell.getTimestamp());
        }
        else if (cell.hasDouble())
        {
            cellBuilder.setDoubleValue(cell.getDouble());
        }
        else
        {
            throw new IllegalArgumentException("No valid cell type found.");
        }
        return cellBuilder;
    }
}
