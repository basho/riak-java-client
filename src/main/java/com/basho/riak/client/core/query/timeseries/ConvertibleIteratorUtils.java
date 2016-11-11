package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.query.ConvertibleIterator;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.Iterator;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
class ConvertibleIteratorUtils
{
    private ConvertibleIteratorUtils(){}

    private static final RiakTsPB.TsCell NullTSCell = RiakTsPB.TsCell.newBuilder().build();

    private static class ImmutablePBCellIterator extends ConvertibleIterator<Cell, RiakTsPB.TsCell>
    {
        public ImmutablePBCellIterator(Iterator<Cell> iterator)
        {
            super(iterator);
        }

        @Override
        protected RiakTsPB.TsCell convert(Cell cell)
        {
            if (cell == null)
            {
                return NullTSCell;
            }

            return cell.getPbCell();
        }
    }

    private static class ImmutablePBRowIterator extends ConvertibleIterator<Row, RiakTsPB.TsRow>
    {
        public ImmutablePBRowIterator(Iterator<Row> iterator)
        {
            super(iterator);
        }

        @Override
        protected RiakTsPB.TsRow convert(Row row)
        {
            return row.getPbRow();
        }
    }

    private static class ImmutableCellIterator extends ConvertibleIterator<RiakTsPB.TsCell, Cell>
    {
        public ImmutableCellIterator(Iterator<RiakTsPB.TsCell> iterator)
        {
            super(iterator);
        }

        @Override
        protected Cell convert(RiakTsPB.TsCell pbCell)
        {
            if (pbCell.equals(NullTSCell))
            {
                return null;
            }

            return new Cell(pbCell);
        }
    }

    private static class ImmutableRowIterator extends ConvertibleIterator<RiakTsPB.TsRow,Row>
    {
        public ImmutableRowIterator(Iterator<RiakTsPB.TsRow> iterator)
        {
            super(iterator);
        }

        @Override
        protected Row convert(RiakTsPB.TsRow source)
        {
            return new Row(source);
        }
    }

    public static ConvertibleIterator<Row, RiakTsPB.TsRow> iterateAsPbRow(Iterator<Row> iterator)
    {
        return new ImmutablePBRowIterator(iterator);
    }

    public static ConvertibleIterator<RiakTsPB.TsRow, Row> iterateAsRow(Iterator<RiakTsPB.TsRow> iterator)
    {
        return new ImmutableRowIterator(iterator);
    }

    public static ConvertibleIterator<Cell, RiakTsPB.TsCell> iterateAsPbCell(Iterator<Cell> iterator)
    {
        return new ImmutablePBCellIterator(iterator);
    }

    public static ConvertibleIterator<RiakTsPB.TsCell, Cell> iterateAsCell(Iterator<RiakTsPB.TsCell> iterator)
    {
        return new ImmutableCellIterator(iterator);
    }
}
