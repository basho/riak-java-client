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

    private QueryResult()
    {
        this.pbRows = Collections.emptyList();
        this.pbColumnDescriptions = Collections.emptyList();
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

    /**
     *
     * @return a deep copy
     */
    public List<ColumnDescription> getColumnDescriptionsCopy()
    {
        return CollectionConverters.convertPBColumnDescriptions(this.pbColumnDescriptions);
    }

    public Iterator<Row> iterator()
    {
        return ConvertibleIterator.iterateAsRow(this.pbRows.iterator());
    }

    public int getRowsCount()
    {
        return this.pbRows.size();
    }

    /**
     *
     * @return a shallow copy
     */
    public List<Row> getRowsCopy()
    {
        final List<Row> rows = new ArrayList<Row>(this.getRowsCount());

        final Iterator<Row> iter = this.iterator();
        while (iter.hasNext())
        {
            rows.add(iter.next());
        }

        return rows;
    }
}
