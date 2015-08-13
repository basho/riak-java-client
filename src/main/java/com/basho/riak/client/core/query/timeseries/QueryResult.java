package com.basho.riak.client.core.query.timeseries;

import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class QueryResult
{
    private List<ColumnDescription> columnDescriptions;
    private List<Row> rows;

    public QueryResult(List<ColumnDescription> columnDescriptions, List<Row> rows) {
        this.columnDescriptions = columnDescriptions;
        this.rows = rows;
    }

    public List<ColumnDescription> getColumnDescriptions()
    {
        return this.columnDescriptions;
    }

    public List<Row> getRows()
    {
        return this.rows;
    }
}
