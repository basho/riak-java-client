package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Converts Time Series Protobuff message types to Java Client entity objects, and back.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public final class TimeSeriesPBConverter
{
    private TimeSeriesPBConverter() {}

    public static QueryResult convertPbGetResp(RiakKvPB.TsQueryResp response)
    {
        if(response == null)
        {
            return QueryResult.emptyResult();
        }

        final List<ColumnDescription> columnDescriptions = convertPBColumnDescriptions(response.getColumnsList());
        final List<Row> rows = convertPbRows(response.getRowsList(), columnDescriptions);

        return new QueryResult(columnDescriptions, rows);
    }

    public static Collection<RiakKvPB.TsColumnDescription> convertColumnDescriptionsToPb(
            Collection<ColumnDescription> columns)
    {
        final ArrayList<RiakKvPB.TsColumnDescription> pbColumns = new ArrayList<RiakKvPB.TsColumnDescription>(columns.size());

        for (ColumnDescription column : columns)
        {
            pbColumns.add(convertColumnDescriptionToPb(column));
        }

        return pbColumns;
    }

    private static RiakKvPB.TsColumnDescription convertColumnDescriptionToPb(ColumnDescription column)
    {
        final RiakKvPB.TsColumnDescription.Builder columnBuilder = RiakKvPB.TsColumnDescription.newBuilder();
        columnBuilder.setName(ByteString.copyFromUtf8(column.getName()));

        columnBuilder.setType(RiakKvPB.TsColumnType.valueOf(column.getType().getId()));

        return columnBuilder.build();
    }

    public static Collection<RiakKvPB.TsRow> convertRowsToPb(Collection<Row> rows)
    {
        final ArrayList<RiakKvPB.TsRow> pbRows = new ArrayList<RiakKvPB.TsRow>(rows.size());

        for (Row row : rows)
        {
            final RiakKvPB.TsRow.Builder rowBuilder = RiakKvPB.TsRow.newBuilder();
            rowBuilder.addAllCells(convertCellsToPb(row.getCells()));
            pbRows.add(rowBuilder.build());
        }

        return pbRows;
    }

    private static ArrayList<RiakKvPB.TsCell> convertCellsToPb(Collection<Cell> cells)
    {
        final ArrayList<RiakKvPB.TsCell> pbCells = new ArrayList<RiakKvPB.TsCell>(cells.size());

        for (Cell cell : cells)
        {
            pbCells.add(convertCellToPb(cell));
        }

        return pbCells;
    }

    private static List<Row> convertPbRows(List<RiakKvPB.TsRow> pbRows, List<ColumnDescription> columnDescriptions)
    {
        if(pbRows == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<Row> rows = new ArrayList<Row>(pbRows.size());

        for (RiakKvPB.TsRow pbRow : pbRows)
        {
            final int numCells = pbRow.getCellsCount();
            final List<Cell> cells = new ArrayList<Cell>(numCells);
            final List<RiakKvPB.TsCell> pbCells = pbRow.getCellsList();

            for (int i = 0; i < numCells; i++)
            {
                final ColumnDescription.ColumnType columnType = columnDescriptions.get(i).getType();
                final RiakKvPB.TsCell pbCell = pbCells.get(i);
                cells.add(convertPbCell(pbCell, columnType));
            }

            rows.add(new Row(cells));
        }

        return rows;
    }

    private static Cell convertPbCell(RiakKvPB.TsCell pbCell, ColumnDescription.ColumnType columnType)
    {
        Cell cell;

        if (columnType == ColumnDescription.ColumnType.BINARY && pbCell.hasBinaryValue())
        {
            cell = new Cell(BinaryValue.unsafeCreate(pbCell.getBinaryValue().toByteArray()));
        }
        else if (columnType == ColumnDescription.ColumnType.BOOLEAN && pbCell.hasBooleanValue())
        {
            cell = new Cell(pbCell.getBooleanValue());
        }
        else if (columnType == ColumnDescription.ColumnType.INTEGER && pbCell.hasIntegerValue())
        {
            cell = new Cell(pbCell.getIntegerValue());
        }
        else if (columnType == ColumnDescription.ColumnType.TIMESTAMP && pbCell.hasIntegerValue())
        {
            cell = Cell.newTimestamp(pbCell.getIntegerValue());
        }
        else if (columnType == ColumnDescription.ColumnType.TIMESTAMP && pbCell.hasTimestampValue())
        {
            cell = Cell.newTimestamp(pbCell.getTimestampValue());
        }
        else if(columnType == ColumnDescription.ColumnType.FLOAT && pbCell.hasFloatValue())
        {
            cell = new Cell(pbCell.getFloatValue());
        }
        else if(columnType == ColumnDescription.ColumnType.FLOAT && pbCell.hasDoubleValue())
        {
            cell = new Cell(pbCell.getDoubleValue());
        }
        else // Null cell
        {
            cell = null;
        }

        return cell;
    }

    private static List<ColumnDescription> convertPBColumnDescriptions(List<RiakKvPB.TsColumnDescription> pbColumns)
    {
        if(pbColumns == null)
        {
            return Collections.emptyList();
        }

        final ArrayList<ColumnDescription> columns = new ArrayList<ColumnDescription>(pbColumns.size());

        for (RiakKvPB.TsColumnDescription pbColumn : pbColumns)
        {

            ColumnDescription columnDescription = convertPBColumnDescription(pbColumn);
            columns.add(columnDescription);
        }

        return columns;
    }

    private static ColumnDescription convertPBColumnDescription(RiakKvPB.TsColumnDescription pbColumn)
    {
        final String name = pbColumn.getName().toStringUtf8();

        final ColumnDescription.ColumnType type = ColumnDescription.ColumnType.valueOf(pbColumn.getType().getNumber());

        return new ColumnDescription(name, type);
    }

    private static RiakKvPB.TsCell convertCellToPb(Cell cell)
    {
        final RiakKvPB.TsCell.Builder cellBuilder = RiakKvPB.TsCell.newBuilder();

        if(cell == null)
        {
            // Return empty cell
            return cellBuilder.build();
        }

        if(cell.hasBinaryValue())
        {
            cellBuilder.setBinaryValue(ByteString.copyFrom(cell.getBinaryValue().unsafeGetValue()));
        }
        else if(cell.hasBoolean())
        {
            cellBuilder.setBooleanValue(cell.getBoolean());
        }
        else if(cell.hasLong())
        {
            cellBuilder.setIntegerValue(cell.getLong());
        }
        else if(cell.hasTimestamp())
        {
            cellBuilder.setTimestampValue(cell.getTimestamp());
        }
        else if(cell.hasDouble())
        {
            cellBuilder.setDoubleValue(cell.getDouble());
        }
        else
        {
            throw new IllegalArgumentException("No valid cell type found.");
        }

        return cellBuilder.build();
    }
}
