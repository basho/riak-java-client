package com.basho.riak.client.core.query.timeseries;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Row implements IRow
{
    private final List<ICell> cells;

    public Row(List<ICell> cells)
    {
        this.cells = cells;
    }

    public Row(ICell... cells)
    {
        this.cells = Arrays.asList(cells);
    }

    public List<ICell> getCells()
    {
        return cells;
    }

    @Override
    public int getCellsCount()
    {
        return cells.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<ICell> iterator()
    {
        return (Iterator)cells.iterator();
    }
}
