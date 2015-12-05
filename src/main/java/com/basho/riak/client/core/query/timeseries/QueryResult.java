package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class QueryResult
{
    public static final QueryResult EMPTY = new QueryResult();
    private final List<RiakTsPB.TsRow> pbRows;
    private final List<RiakTsPB.TsColumnDescription> pbColumnDescriptions;
    private transient List<Row> rows;
    private transient List<ColumnDescription> columns;

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
    }

    @SuppressWarnings("unchecked")
    public List<ColumnDescription> getColumnDescriptions()
    {
        if (columns == null)
        {
            columns =
                    Collections.unmodifiableList((List) CollectionConverters.convertPBColumnDescriptions(this.pbColumnDescriptions));
        }
        return columns;
    }

    public Iterator<Row> iterator()
    {
        return new ImmutableRowIterator(this.pbRows);
    }

    public int getRowsCount()
    {
        return this.pbRows.size();
    }

    public List<Row> getRowsListCopy()
    {
        if (this.rows == null)
        {
            final Iterator<Row> iter = this.iterator();
            while (iter.hasNext())
            {
                this.rows.add(iter.next());
            }
        }

        return this.rows;
    }

    private static class ImmutableRowIterator implements Iterator<Row>
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
        public Row next()
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
