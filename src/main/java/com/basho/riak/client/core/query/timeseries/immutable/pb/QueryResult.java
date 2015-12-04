package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.query.timeseries.IColumnDescription;
import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.client.core.query.timeseries.IRow;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
class QueryResult implements IQueryResult
{
    public static final QueryResult EmptyResult = new QueryResult();

    private final List<RiakTsPB.TsRow> pbRows;
    private final List<RiakTsPB.TsColumnDescription> pbColumnDescriptions;
    private final transient List<IRow> rows;
    private transient List<IColumnDescription> columns;

    private QueryResult()
    {
        this.pbRows = Collections.emptyList();
        this.pbColumnDescriptions = Collections.emptyList();
        this.rows = Collections.emptyList();
    }

    public QueryResult(List<RiakTsPB.TsRow> tsRows)
    {
        this(Collections.<RiakTsPB.TsColumnDescription>emptyList(), tsRows);
    }

    public QueryResult(List<RiakTsPB.TsColumnDescription> columnsList, List<RiakTsPB.TsRow> rowsList)
    {
        this.pbColumnDescriptions = columnsList;
        this.pbRows = rowsList;
        this.rows = new ArrayList<IRow>(this.pbRows.size());
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IColumnDescription> getColumnDescriptions()
    {
        if (columns == null)
        {
            columns =
                    Collections.unmodifiableList((List) TimeSeriesPBConverter.convertPBColumnDescriptions(this.pbColumnDescriptions));
        }
        return columns;
    }

    @Override
    public Iterator<IRow> iterator()
    {
        return new ImmutableRowIterator(this.pbRows);
    }

    @Override
    public int getRowsCount()
    {
        return this.pbRows.size();
    }

    @Override
    public List<IRow> getRows()
    {
        if (this.rows.size() != this.getRowsCount())
        {
            final Iterator<IRow> iter = this.iterator();
            while (iter.hasNext())
            {
                this.rows.add(iter.next());
            }
        }

        return this.rows;
    }

    private static class ImmutableRowIterator implements Iterator<IRow>
    {
        private Iterator<RiakTsPB.TsRow> itor;

        public ImmutableRowIterator(List<RiakTsPB.TsRow> cells)
        {
            this.itor = cells.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return itor.hasNext();
        }

        @Override
        public IRow next()
        {
            return new Row(itor.next());
        }

        @Override
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    }
}
