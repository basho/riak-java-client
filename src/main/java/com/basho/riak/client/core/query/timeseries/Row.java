package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class Row
{
    private final List<Cell> cells;

    public Row(List<Cell> cells)
    {
        this.cells = cells;
    }

    public List<Cell> getCells() {
        return cells;
    }

    public Iterator<Cell> iterator()
    {
        return this.cells.iterator();
    }
}
