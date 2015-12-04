package com.basho.riak.client.core.query.timeseries;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public interface IColumnDescription
{
    /**
     * Values MUST BE IN THE SAME ORDER AS in the RiakTsPB.TsColumnType
     */
    enum ColumnType {
        VARCHAR,
        SINT64,
        DOUBLE,
        TIMESTAMP,
        BOOLEAN
    }

    String getName();
    ColumnType getType();
}
