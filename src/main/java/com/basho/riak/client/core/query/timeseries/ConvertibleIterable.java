package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.Iterator;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public abstract class ConvertibleIterable<S,D> implements Iterable<D>
{
    protected final Iterable<S> source;

    public ConvertibleIterable(Iterable<S> source)
    {
        this.source = source;
    }

    private static class ImmutableIterablePBCell extends ConvertibleIterable<Cell, RiakTsPB.TsCell>
    {
        public ImmutableIterablePBCell(Iterable<Cell> source)
        {
            super(source);
        }

        @Override
        public Iterator<RiakTsPB.TsCell> iterator()
        {
            return ConvertibleIterator.iterateAsPbCell(this.source.iterator());
        }
    }

    private static class ImmutableIterablePBRow extends ConvertibleIterable<Row, RiakTsPB.TsRow>
    {
        public ImmutableIterablePBRow(Iterable<Row> source)
        {
            super(source);
        }

        @Override
        public Iterator<RiakTsPB.TsRow> iterator()
        {
            return ConvertibleIterator.iterateAsPbRow(this.source.iterator());
        }
    }

    private static class ImmutableIterableRow extends ConvertibleIterable<RiakTsPB.TsRow, Row>
    {
        public ImmutableIterableRow(Iterable<RiakTsPB.TsRow> source)
        {
            super(source);
        }

        @Override
        public Iterator<Row> iterator()
        {
            return ConvertibleIterator.iterateAsRow(this.source.iterator());
        }
    }

    private static class ImmutableIterableCell extends ConvertibleIterable<RiakTsPB.TsCell, Cell>
    {
        public ImmutableIterableCell(Iterable<RiakTsPB.TsCell> source)
        {
            super(source);
        }

        @Override
        public Iterator<Cell> iterator()
        {
            return ConvertibleIterator.iterateAsCell(this.source.iterator());
        }
    }

    public static ConvertibleIterable<Row, RiakTsPB.TsRow> asIterablePbRow(Iterable<Row> iterable)
    {
        return new ImmutableIterablePBRow(iterable);
    }

    public static ConvertibleIterable<RiakTsPB.TsRow, Row> asIterableRow(Iterable<RiakTsPB.TsRow> iterable)
    {
        return new ImmutableIterableRow(iterable);
    }

    public static ConvertibleIterable<Cell, RiakTsPB.TsCell> asIterablePbCell(Iterable<Cell> iterable)
    {
        return new ImmutableIterablePBCell(iterable);
    }

    public static ConvertibleIterable<RiakTsPB.TsCell, Cell> asIterableCell(Iterable<RiakTsPB.TsCell> iterable)
    {
        return new ImmutableIterableCell(iterable);
    }
}
