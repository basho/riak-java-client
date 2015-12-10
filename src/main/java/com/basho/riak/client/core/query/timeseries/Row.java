package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.*;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Row implements Iterable<Cell>
{
    private final RiakTsPB.TsRow pbRow;

    public Row(Collection<Cell> cells)
    {
        final RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();

        builder.addAllCells(ConvertibleIterable.asIterablePbCell(cells));

        this.pbRow = builder.build();
    }

    public Row(Cell... cells)
    {
        final RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();
        // TODO: consider avoiding ArrayList creation under the hood of Arrays.asList
        builder.addAllCells(ConvertibleIterable.asIterablePbCell(Arrays.asList(cells)));

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

    /**
     *
     * @return a shallow copy
     */
    public List<Cell> getCellsCopy()
    {
        final ArrayList<Cell> cells = new ArrayList<Cell>(this.getCellsCount());

        for (Cell c: this)
        {
            cells.add(c);
        }
        return cells;
    }

    RiakTsPB.TsRow getPbRow()
    {
        return pbRow;
    }

    @Override
    public Iterator<Cell> iterator()
    {
        return ConvertibleIterator.iterateAsCell(pbRow.getCellsList().iterator());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        Row cells = (Row) o;

        return !(pbRow != null ? !pbRow.equals(cells.pbRow) : cells.pbRow != null);

    }

    @Override
    public int hashCode()
    {
        return pbRow != null ? pbRow.hashCode() : 0;
    }
}
