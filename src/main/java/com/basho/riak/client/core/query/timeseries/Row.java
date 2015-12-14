package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.*;

/**
 * Holds a collection of Cells, grouped by a primary key in Riak.
 * Immutable once created.
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Row implements Iterable<Cell>
{
    private final RiakTsPB.TsRow pbRow;

    /**
     * Create a new row.
     * @param cells the collection of cells that make up the row.
     */
    public Row(Iterable<Cell> cells)
    {
        final RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();

        builder.addAllCells(ConvertibleIterable.asIterablePbCell(cells));

        this.pbRow = builder.build();
    }

    /**
     * Create a new row.
     * @param cells the varargs collection of cells that make up the row.
     */
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

    /**
     * Get the number of cells in this row.
     * @return the count of cells in this row.
     */
    public int getCellsCount()
    {
        return pbRow.getCellsCount();
    }

    /**
     * Get a shallow copy of the cells in this row.
     * @return a List&lt;Cell&gt; shallow copy of the cells in this row.
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

    /**
     * An iterator of the Cells in this row.
     * @return an iterator.
     */
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
