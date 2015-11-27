package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.*;

/**
 * Converts Time Series Protobuff message types to Java Client entity objects, and back.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public final class TimeSeriesPBConverter
{
    private TimeSeriesPBConverter() {}

    public static QueryResult convertPbGetResp(RiakTsPB.TsQueryResp response)
    {
        if (response == null)
        {
            return QueryResult.emptyResult();
        }

        final List<ColumnDescription> columnDescriptions = convertPBColumnDescriptions(response.getColumnsList());
        final List<Row> rows = convertPbRows(response.getRowsList(), columnDescriptions);

        return new QueryResult(columnDescriptions, rows);
    }

    public static QueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        if (response == null)
        {
            return QueryResult.emptyResult();
        }

        final List<ColumnDescription> columnDescriptions = convertPBColumnDescriptions(response.getColumnsList());
        final List<Row> rows = convertPbRows(response.getRowsList(), columnDescriptions);

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

        columnBuilder.setType(RiakTsPB.TsColumnType.valueOf(column.getType().getId()));

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
            for (Cell cell : row.getCells())
            {
                pbCells[idx++] = buildPBCell(cellBuilder, cell).build();
            }

            rowBuilder.addAllCells(Arrays.asList(pbCells));
            pbRows.add(rowBuilder.build());
            rowBuilder.clear();
        }
        return pbRows;
    }

    public static List<RiakTsPB.TsCell> convertCellsToPb(RiakTsPB.TsCell.Builder cellBuilder, Collection<Cell> cells)
    {
        final ArrayList<RiakTsPB.TsCell> pbCells = new ArrayList<RiakTsPB.TsCell>(cells.size());

        for (Cell cell : cells)
        {
            pbCells.add(buildPBCell(cellBuilder, cell).build());
        }

        return pbCells;
    }

    public static List<RiakTsPB.TsCell> convertCellsToPb(Collection<Cell> cells)
    {
        return convertCellsToPb(RiakTsPB.TsCell.newBuilder(), cells);
    }

    private static List<Row> convertPbRows(List<RiakTsPB.TsRow> pbRows, List<ColumnDescription> columnDescriptions)
    {
        if (pbRows == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<Row> rows = new ArrayList<Row>(pbRows.size());

        for (RiakTsPB.TsRow pbRow : pbRows)
        {
            final int numCells = pbRow.getCellsCount();
            final List<Cell> cells = new ArrayList<Cell>(numCells);
            final List<RiakTsPB.TsCell> pbCells = pbRow.getCellsList();

            for (int i = 0; i < numCells; i++)
            {
                final ColumnDescription.ColumnType columnType = columnDescriptions.get(i).getType();
                final RiakTsPB.TsCell pbCell = pbCells.get(i);
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

    private static List<ColumnDescription> convertPBColumnDescriptions(List<RiakTsPB.TsColumnDescription> pbColumns)
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

        final ColumnDescription.ColumnType type = ColumnDescription.ColumnType.valueOf(pbColumn.getType().getNumber());

        return new ColumnDescription(name, type);
    }

    private static RiakTsPB.TsCell.Builder buildPBCell(RiakTsPB.TsCell.Builder cellBuilder, Cell cell) {
        cellBuilder.clear();

        if (cell == null) {
            return cellBuilder;
        } else if (cell.hasVarcharValue())
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
