package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.query.timeseries.ICell;
import com.basho.riak.client.core.query.timeseries.IRow;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
class Row implements IRow
{
    private static class ImmutableCellIterator implements Iterator<ICell>
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
        public ICell next()
        {
            return new Cell(itor.next());
        }

        @Override
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    }
    
    private final RiakTsPB.TsRow pbRow;

    private transient final List<ICell> cells;

    public Row(RiakTsPB.TsRow pbRow)
    {
        this.pbRow = pbRow;
        this.cells = new ArrayList<ICell>(pbRow.getCellsCount());
    }

    @Override
    public int getCellsCount()
    {
        return pbRow.getCellsCount();
    }

    @Override
    public List<ICell> getCells()
    {
        if (this.cells.size() != this.getCellsCount())
        {
            final Iterator<ICell> iter = this.iterator();
            while (iter.hasNext())
            {
                this.cells.add(iter.next());
            }
        }
        
        return this.cells;
    }

    @Override
    public Iterator<ICell> iterator()
    {
        return new ImmutableCellIterator(pbRow.getCellsList());
    }
}
