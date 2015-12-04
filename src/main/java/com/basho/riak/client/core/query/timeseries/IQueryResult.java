package com.basho.riak.client.core.query.timeseries;

import javax.naming.directory.Attribute;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public interface IQueryResult extends Iterable<IRow>
{
    List<IColumnDescription> getColumnDescriptions();
    int getRowsCount();
    List<IRow> getRows();
}
