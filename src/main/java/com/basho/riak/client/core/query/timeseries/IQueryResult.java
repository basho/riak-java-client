package com.basho.riak.client.core.query.timeseries;

import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
// TODO: Consider implementation of Iterable<IRow>
public interface IQueryResult
{
    List<IColumnDescription> getColumnDescriptions();
    Iterator<IRow> rows();
    int getRowsCount();
}
