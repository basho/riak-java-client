package com.basho.riak.client.core.query.timeseries;

import java.util.HashMap;
import java.util.Map;

/**
 * A Metadata description of a column in Riak Time Series.
 * Contains a column name and column type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ColumnDescription
{
    private final String name;
    private final ColumnType type;

    public ColumnDescription(String name, ColumnType type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public ColumnType getType()
    {
        return type;
    }


    public enum ColumnType
    {
        /**
         * Values MUST BE IN THE SAME ORDER AS in the RiakTsPB.TsColumnType
         */
        VARCHAR,
        SINT64,
        DOUBLE,
        TIMESTAMP,
        BOOLEAN
    }
}
