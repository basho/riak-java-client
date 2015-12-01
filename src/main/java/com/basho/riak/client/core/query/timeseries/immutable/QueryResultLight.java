package com.basho.riak.client.core.query.timeseries.immutable;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.query.timeseries.IColumnDescription;
import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.client.core.query.timeseries.IRow;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
class QueryResultLight implements IQueryResult
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
            return new RowLight(itor.next());
        }

        @Override
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    }

    private final RiakTsPB.TsQueryResp pbResponse;
    private transient List<IColumnDescription> columns;

    public QueryResultLight(RiakTsPB.TsQueryResp response)
    {
        this.pbResponse = response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IColumnDescription> getColumnDescriptions()
    {
        if (columns == null)
        {
            columns = Collections.unmodifiableList(
                    (List)TimeSeriesPBConverter.convertPBColumnDescriptions(pbResponse.getColumnsList()));
        }
        return columns;
    }

    @Override
    public Iterator<IRow> rows()
    {
        return new ImmutableRowIterator(pbResponse.getRowsList());
    }

    @Override
    public int getRowsCount()
    {
        return pbResponse.getRowsCount();
    }
}
