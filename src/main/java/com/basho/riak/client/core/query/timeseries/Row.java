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
    private final Iterable<Cell> cells;
    private final int cellCount;

    /**
     * Create a new row.
     * @param cells the collection of cells that make up the row.
     */
    public Row(Iterable<Cell> cells)
    {
        pbRow = null;
        this.cells = cells;
        int cellCount = 0;

        for (Cell ignored : this.cells)
        {
            cellCount++;
        }

        this.cellCount = cellCount;
    }

    /**
     * Create a new row.
     * @param cells the varargs collection of cells that make up the row.
     */
    public Row(Cell... cells)
    {
        pbRow = null;
        this.cells = Arrays.asList(cells);
        cellCount = cells.length;
    }

    Row(RiakTsPB.TsRow pbRow)
    {
        this.pbRow = pbRow;
        cells = null;
        cellCount = pbRow.getCellsCount();
    }

    /**
     * Get the number of cells in this row.
     * @return the count of cells in this row.
     */
    public int getCellsCount()
    {
        return cellCount;
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

    public RiakTsPB.TsRow getPbRow()
    {
        if(pbRow != null)
        {
            return pbRow;
        }

        RiakTsPB.TsRow.Builder builder = RiakTsPB.TsRow.newBuilder();
        builder.addAllCells(ConvertibleIterable.asIterablePbCell(cells));
        return builder.build();
    }

    /**
     * An iterator of the Cells in this row.
     * @return an iterator.
     */
    @Override
    public Iterator<Cell> iterator()
    {
        if(cells != null)
        {
            return cells.iterator();
        }
        else // if (pbRow != null)
        {
            return ConvertibleIterator.iterateAsCell(pbRow.getCellsList().iterator());
        }
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

        Row cells1 = (Row) o;

        if (cellCount != cells1.cellCount)
        {
            return false;
        }
        return getCellsCopy().equals(cells1.getCellsCopy());

    }

    @Override
    public int hashCode()
    {
        int result = pbRow != null ? pbRow.hashCode() : 0;
        result = 31 * result + (cells != null ? cells.hashCode() : 0);
        result = 31 * result + cellCount;
        return result;
    }
}
