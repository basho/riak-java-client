package com.basho.riak.client.core.query.timeseries;

import java.util.HashMap;
import java.util.Map;

/**
 * A Metadata description of a column in Riak Time Series.
 * Contains a column name and column type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class ColumnDescription implements IColumnDescription
{
    private final String name;
    private final ColumnType type;

    public ColumnDescription(String name, ColumnType type)
    {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ColumnType getType()
    {
        return type;
    }
}
