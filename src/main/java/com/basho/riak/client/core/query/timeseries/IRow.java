package com.basho.riak.client.core.query.timeseries;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public interface IRow extends Iterable<ICell>
{
    int getCellsCount();
}
