package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.protobuf.RiakKvPB;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class TimeSeriesConverter
{
    private List<Row> parseRows(List<RiakKvPB.TsRow> pbRows)
    {
        ArrayList<Row> rows = new ArrayList<Row>();

        for (RiakKvPB.TsRow pbRow : pbRows) {

            List<Cell> cells = new ArrayList<Cell>();

            for (RiakKvPB.TsCell pbCell : pbRow.getCellsList()) {
                if(pbCell.hasBinaryValue())
                {
                    cells.add(Cell.newBinaryCell(pbCell.getBinaryValue().toByteArray()));
                }
                else if(pbCell.hasBooleanValue())
                {
                    cells.add(Cell.newBooleanCell(pbCell.getBooleanValue()));
                }
                else if(pbCell.hasIntegerValue())
                {
                    cells.add(Cell.newIntegerCell(pbCell.getIntegerValue()));
                }
                else if(pbCell.hasMapValue())
                {
                    cells.add(Cell.newMapCell(pbCell.getMapValue().toByteArray()));
                }
                else if(pbCell.hasNumericValue())
                {
                    cells.add(Cell.newNumericCell(pbCell.getNumericValue().toByteArray()));
                }
                else if(pbCell.hasTimestampValue())
                {
                    cells.add(Cell.newTimestampCell(pbCell.getTimestampValue()));
                }
                else
                {
                    int size = pbCell.getSetValueCount();
                    byte[][] set = new byte[size][];

                    for(int i = 0; i < size; i++)
                    {
                        set[i] = pbCell.getSetValue(i).toByteArray();
                    }

                    Cell.newSetCell(set);
                }
            }

            rows.add(new Row(cells));
        }

        return rows;
    }

    private List<ColumnDescription> parseColumnDescriptions(List<RiakKvPB.TsColumnDescription> pbColumns)
    {
        ArrayList<ColumnDescription> columns = new ArrayList<ColumnDescription>();

        for (RiakKvPB.TsColumnDescription pbColumn : pbColumns) {

            String name = pbColumn.getName().toStringUtf8();

            ColumnDescription.ColumnType type = ColumnDescription.ColumnType.valueOf(pbColumn.getType().getNumber());
            List<ColumnDescription.ColumnType> complexType = new ArrayList<ColumnDescription.ColumnType>();

            for (RiakKvPB.TsColumnType pbComplexType : pbColumn.getComplexTypeList()) {
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
}
