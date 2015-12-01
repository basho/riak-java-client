package com.basho.riak.client.core.query.timeseries;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
*/
public class QueryResult implements IQueryResult
{
    @SuppressWarnings("unchecked")
    public static final QueryResult EMPTY = new QueryResult(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    private final List<ColumnDescription> columnDescriptions;
    private final List<Row> rows;

    public QueryResult(List<ColumnDescription> columnDescriptions, List<Row> rows) {
        this.columnDescriptions = columnDescriptions;
        this.rows = rows;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IColumnDescription> getColumnDescriptions()
    {
        return (List)this.columnDescriptions;
    }

    public List<Row> getRows()
    {
        return rows;
    }


    @SuppressWarnings("unchecked")
    @Override
    public Iterator<IRow> rows()
    {
        return (Iterator)this.rows.iterator();
    }

    @Override
    public int getRowsCount()
    {
        return this.rows.size();
    }
}
