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
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class TimeSeriesConverter
{
    private List<Row> parseRows(List<RiakKvPB.TsRow> pbRows)
    {
        ArrayList<Row> rows = new ArrayList<Row>();

        for (RiakKvPB.TsRow pbRow : pbRows)
        {
            List<Cell> cells = new ArrayList<Cell>();

            for (RiakKvPB.TsCell pbCell : pbRow.getCellsList())
            {
                if (pbCell.hasBinaryValue())
                {
                    cells.add(new Cell(BinaryValue.unsafeCreate(pbCell.getBinaryValue().toByteArray())));
                }
                else if (pbCell.hasBooleanValue())
                {
                    cells.add(new Cell(pbCell.getBooleanValue()));
                }
                else if (pbCell.hasIntegerValue())
                {
                    cells.add(new Cell(pbCell.getIntegerValue()));
                }
                else if (pbCell.hasMapValue())
                {
                    cells.add(Cell.newMap(pbCell.getMapValue().toByteArray()));
                }
                else if (pbCell.hasNumericValue())
                {
                    cells.add(Cell.newRawNumeric(pbCell.getNumericValue().toByteArray()));
                }
                else if (pbCell.hasTimestampValue())
                {
                    cells.add(Cell.newTimestamp(pbCell.getTimestampValue()));
                }
                else // Set
                {
                    int size = pbCell.getSetValueCount();
                    byte[][] set = new byte[size][];

                    for (int i = 0; i < size; i++)
                    {
                        set[i] = pbCell.getSetValue(i).toByteArray();
                    }

                    Cell.newSet(set);
                }
            }

            rows.add(new Row(cells));
        }

        return rows;
    }

    private List<ColumnDescription> parseColumnDescriptions(List<RiakKvPB.TsColumnDescription> pbColumns)
    {
        ArrayList<ColumnDescription> columns = new ArrayList<ColumnDescription>();

        for (RiakKvPB.TsColumnDescription pbColumn : pbColumns)
        {

            String name = pbColumn.getName().toStringUtf8();

            ColumnDescription.ColumnType type = ColumnDescription.ColumnType.valueOf(pbColumn.getType().getNumber());
            List<ColumnDescription.ColumnType> complexType = new ArrayList<ColumnDescription.ColumnType>();

            for (RiakKvPB.TsColumnType pbComplexType : pbColumn.getComplexTypeList())
            {
                complexType.add(ColumnDescription.ColumnType.valueOf(pbComplexType.getNumber()));
            }

            columns.add(new ColumnDescription(name, type, complexType));
        }

        return columns;
    }

    public QueryResult convert(RiakKvPB.TsQueryResp response)
    {
        List<ColumnDescription> columnDescriptions = parseColumnDescriptions(response.getColumnsList());
        List<Row> rows = parseRows(response.getRowsList());

        return new QueryResult(columnDescriptions, rows);
    }

    public Collection<RiakKvPB.TsColumnDescription> convertColumns(Collection<ColumnDescription> columns)
    {
        ArrayList<RiakKvPB.TsColumnDescription> pbColumns = new ArrayList<RiakKvPB.TsColumnDescription>(columns.size());

        for (ColumnDescription column : columns)
        {
            pbColumns.add(convertColumn(column));
        }

        return pbColumns;
    }

    public Collection<RiakKvPB.TsRow> convertRows(Collection<Row> rows)
    {
        ArrayList<RiakKvPB.TsRow> pbRows = new ArrayList<RiakKvPB.TsRow>(rows.size());

        for (Row row : rows)
        {
            pbRows.add(convertRow(row));
        }

        return pbRows;
    }

    public RiakKvPB.TsRow convertRow(Row row)
    {
        RiakKvPB.TsRow.Builder rowBuilder = RiakKvPB.TsRow.newBuilder();

        for (Cell cell : row.getCells())
        {
            rowBuilder.addCells(convertCell(cell));
        }

        return rowBuilder.build();
    }

    private RiakKvPB.TsCell convertCell(Cell cell)
    {
        RiakKvPB.TsCell.Builder cellBuilder = RiakKvPB.TsCell.newBuilder();

        if(cell.hasBinaryValue())
        {
            cellBuilder.setBinaryValue(ByteString.copyFrom(cell.getBinaryValue().unsafeGetValue()));
        }
        else if(cell.hasBoolean())
        {
            cellBuilder.setBooleanValue(cell.getBoolean());
        }
        else if(cell.hasInt())
        {
            cellBuilder.setIntegerValue(cell.getLong());
        }
        else if(cell.hasMap())
        {
            cellBuilder.setMapValue(ByteString.copyFrom(cell.getMap()));
        }
        else if(cell.hasNumeric())
        {
            cellBuilder.setNumericValue(ByteString.copyFrom(cell.getRawNumeric()));
        }
        else if(cell.hasTimestamp())
        {
            cellBuilder.setTimestampValue(cell.getTimestamp());
        }
        else // Set
        {
            int i = cellBuilder.getSetValueCount();
            for (byte[] setMember : cell.getSet())
            {
                cellBuilder.setSetValue(i, ByteString.copyFrom(setMember));
                i++;
            }
        }

        return cellBuilder.build();
    }

    public RiakKvPB.TsColumnDescription convertColumn(ColumnDescription column)
    {
        RiakKvPB.TsColumnDescription.Builder columnBuilder = RiakKvPB.TsColumnDescription.newBuilder();
        columnBuilder.setName(ByteString.copyFromUtf8(column.getName()));

        if(column.getType() != null)
        {
            columnBuilder.setType(RiakKvPB.TsColumnType.valueOf(column.getType().getId()));
        }

        Collection<ColumnDescription.ColumnType> complexType = column.getComplexType();
        if(complexType != null)
        {
            for (ColumnDescription.ColumnType complexTypePart : complexType)
            {
                columnBuilder.addComplexType(RiakKvPB.TsColumnType.valueOf(complexTypePart.getId()));
            }
        }
        return columnBuilder.build();
    }
}
