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

    private abstract static class ConvertibleZipperator<S, S2, D> implements Iterator<D>
    {
        protected final Iterator<S> iterator1;
        protected final Iterator<S2> iterator2;

        public ConvertibleZipperator(Iterator<S> iterator, Iterator<S2> iterator2)
        {
            this.iterator1 = iterator;
            this.iterator2 = iterator2;
        }

        abstract protected D convert(S source, S2 source2);

        @Override
        public boolean hasNext() {
            return iterator1.hasNext() && iterator2.hasNext();
        }

        @Override
        public D next() {
            return convert(iterator1.next(), iterator2.next());
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class ImmutableCellIterator extends ConvertibleZipperator<RiakTsPB.TsCell, RiakTsPB.TsColumnDescription, Cell>
    {
        public ImmutableCellIterator(Iterator<RiakTsPB.TsCell> cellIterator,
                                     Iterator<RiakTsPB.TsColumnDescription> descriptionIterator )
        {
            super(cellIterator, descriptionIterator);
        }

        @Override
        protected Cell convert(RiakTsPB.TsCell pbCell, RiakTsPB.TsColumnDescription columnDescription)
        {
            if (pbCell.equals(NullTSCell))
            {
                return null;
            }

            return new Cell(pbCell, columnDescription);
        }

        @Override
        public boolean hasNext() {
            return iterator1.hasNext();
        }

        @Override
        public Cell next() {
            final RiakTsPB.TsColumnDescription description = iterator2.hasNext() ? iterator2.next() : null;
            return convert(iterator1.next(), description);
        }
    }

    private static class ImmutableRowIterator extends ConvertibleIterator<RiakTsPB.TsRow, Row>
    {
        private final Iterable<RiakTsPB.TsColumnDescription> columnDescriptions;

        public ImmutableRowIterator(Iterator<RiakTsPB.TsRow> iterator,
                                    Iterable<RiakTsPB.TsColumnDescription> columnDescriptions)
        {
            super(iterator);
            this.columnDescriptions = columnDescriptions;
        }

        @Override
        protected Row convert(RiakTsPB.TsRow source)
        {
            return new Row(source, columnDescriptions);
        }
    }

    public static ConvertibleIterator<RiakTsPB.TsRow, Row> iterateAsRow(Iterator<RiakTsPB.TsRow> iterator,
                                                                        Iterable<RiakTsPB.TsColumnDescription> columnDescriptions)
    {
        return new ImmutableRowIterator(iterator, columnDescriptions);
    }

    public static ConvertibleIterator<Cell, RiakTsPB.TsCell> iterateAsPbCell(Iterator<Cell> iterator)
    {
        return new ImmutablePBCellIterator(iterator);
    }

    public static ConvertibleZipperator<RiakTsPB.TsCell, RiakTsPB.TsColumnDescription, Cell>
        iterateAsCell(Iterator<RiakTsPB.TsCell> cellIterator,
                      Iterator<RiakTsPB.TsColumnDescription> columnDescriptionIterator)
    {
        return new ImmutableCellIterator(cellIterator, columnDescriptionIterator);
    }
}
