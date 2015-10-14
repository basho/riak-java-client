package com.basho.riak.client.core.query.timeseries;

import java.util.Arrays;
import java.util.List;

/**
 * Describes a row of data in a Riak Time Series table.
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

    public Row(Cell... cells)
    {
        this(Arrays.asList(cells));
    }

    /**
     * Get the cells this row contains.
     * @return The cells of this row.
     */
    public List<Cell> getCells()
    {
        return cells;
    }
}
