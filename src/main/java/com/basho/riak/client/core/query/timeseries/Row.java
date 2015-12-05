package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Row
{
    private final RiakTsPB.TsRow pbRow;
    private transient List<Cell> cells;

    public Row(List<Cell> cells)
    {
        final RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();

        builder.addAllCells(CollectionConverters.convertCellsToPb(cells));

        this.pbRow = builder.build();
    }

    public Row(Cell... cells)
    {
        final RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();
        builder.addAllCells(CollectionConverters.convertCellsToPb(Arrays.asList(cells)));

        this.pbRow = builder.build();
    }

    Row(RiakTsPB.TsRow pbRow)
    {
        this.pbRow = pbRow;
    }

    public int getCellsCount()
    {
        return pbRow.getCellsCount();
    }

    public List<Cell> getCellsListCopy()
    {
        if (this.cells == null)
        {
            this.cells = new ArrayList<Cell>(this.getCellsCount());

            final Iterator<Cell> iter = this.iterator();
            while (iter.hasNext())
            {
                this.cells.add(iter.next());
            }
        }

        return this.cells;
    }

    RiakTsPB.TsRow getPbRow()
    {
        return pbRow;
    }

    public Iterator<Cell> iterator()
    {
        return new ImmutableCellIterator(pbRow.getCellsList());
    }

    private static class ImmutableCellIterator implements Iterator<Cell>
    {
        private Iterator<RiakTsPB.TsCell> itor;

        public ImmutableCellIterator(List<RiakTsPB.TsCell> cells)
        {
            this.itor = cells.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return itor.hasNext();
        }

        @Override
        public Cell next()
        {
            return new Cell(itor.next());
        }

        @Override
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    }
}
