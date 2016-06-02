package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Holds a result set from a query, keylist, or fetch command.
 * Immutable once created.
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class QueryResult implements Iterable<Row>
{
    public static final QueryResult EMPTY = new QueryResult();
    private final Iterable<RiakTsPB.TsRow> pbRows;
    private final Row[] rows;
    private final int rowCount;
    private final List<RiakTsPB.TsColumnDescription> pbColumnDescriptions;

    private QueryResult()
    {
        this.pbRows = Collections.emptyList();
        this.rowCount = 0;
        this.pbColumnDescriptions = Collections.emptyList();
        this.rows = null;
    }

    public QueryResult(List<RiakTsPB.TsRow> tsRows)
    {
        this(Collections.<RiakTsPB.TsColumnDescription>emptyList(), tsRows);
    }

    public QueryResult(Iterable<RiakTsPB.TsRow> tsRowsIterator, int rowCount)
    {
        this.pbColumnDescriptions = null;
        this.pbRows = tsRowsIterator;
        this.rowCount = rowCount;
        this.rows = null;
    }

    public QueryResult(List<RiakTsPB.TsColumnDescription> columnsList, List<RiakTsPB.TsRow> rowsList)
    {
        this.pbColumnDescriptions = columnsList;
        this.pbRows = rowsList;
        this.rowCount = rowsList.size();
        this.rows = null;
    }

    public QueryResult(Row[] rows)
    {
        this.rows = rows;
        this.rowCount = rows.length;
        this.pbRows = Collections.emptyList();
        this.pbColumnDescriptions = Collections.emptyList();
    }

    /**
     * Provides a deep copy of the ColumnDescription List, if one was returned from the operation.
     * @return a deep copy of the ColumnDescriptions
     */
    public List<ColumnDescription> getColumnDescriptionsCopy()
    {
        return CollectionConverters.convertPBColumnDescriptions(this.pbColumnDescriptions);
    }

    /**
     * An iterator of the Rows in this QueryResult.
     * @return an iterator.
     */
    public Iterator<Row> iterator()
    {
        if (this.rows != null)
        {
            return Arrays.asList(this.rows).iterator();
        }
        else
        {
            return ConvertibleIterator.iterateAsRow(this.pbRows.iterator());
        }
    }

    /**
     * Get the number of rows in this query result.
     * @return the count of rows in this query result.
     */
    public int getRowsCount()
    {
        return this.rowCount;
    }

    /**
     * Get a shallow copy of the rows in this query result.
     * @return a List&lt;Row&gt; shallow copy of the rows in this query result.
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
