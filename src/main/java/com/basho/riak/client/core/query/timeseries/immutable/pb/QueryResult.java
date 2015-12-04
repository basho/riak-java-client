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

    private final List<RiakTsPB.TsRow> pbRows;
    private final List<RiakTsPB.TsColumnDescription> pbColumnDescriptions;
    private transient List<IColumnDescription> columns;

    public QueryResult(List<RiakTsPB.TsListKeysResp> responseChunks)
    {
        int size = 0;
        for (RiakTsPB.TsListKeysResp resp : responseChunks)
        {
            size += resp.getKeysCount();
        }

        this.pbRows = new ArrayList<RiakTsPB.TsRow>(size);

        for (RiakTsPB.TsListKeysResp resp : responseChunks)
        {
            this.pbRows.addAll(resp.getKeysList());
        }

        this.pbColumnDescriptions = null;
    }

    public QueryResult(RiakTsPB.TsQueryResp queryResp)
    {
        this.pbRows = queryResp.getRowsList();
        this.pbColumnDescriptions = queryResp.getColumnsList();
    }

    public QueryResult(RiakTsPB.TsGetResp getResp)
    {
        this.pbRows = getResp.getRowsList();
        this.pbColumnDescriptions = getResp.getColumnsList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IColumnDescription> getColumnDescriptions()
    {
        if (columns == null)
        {
            columns = Collections.unmodifiableList(
                    (List)TimeSeriesPBConverter.convertPBColumnDescriptions(this.pbColumnDescriptions));
        }
        return columns;
    }

    @Override
    public Iterator<IRow> rows()
    {
        return new ImmutableRowIterator(this.pbRows);
    }

    @Override
    public int getRowsCount()
    {
        return this.pbRows.size();
    }
}
