package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.Iterator;

public abstract class ConvertibleIterable<S,D> implements Iterable<D>
{
    protected final Iterable<S> source;

    public ConvertibleIterable(Iterable<S> source)
    {
        this.source = source;
    }

    private static class ImmutablePBCellIterable extends ConvertibleIterable<Cell, RiakTsPB.TsCell>
    {
        public ImmutablePBCellIterable(Iterable<Cell> source)
        {
            super(source);
        }

        @Override
        public Iterator<RiakTsPB.TsCell> iterator()
        {
            return ConvertibleIterator.iterateAsPbCell(this.source.iterator());
        }
    }

    private static class ImmutablePBRowIterable extends ConvertibleIterable<Row, RiakTsPB.TsRow>
    {
        public ImmutablePBRowIterable(Iterable<Row> source)
        {
            super(source);
        }

        @Override
        public Iterator<RiakTsPB.TsRow> iterator()
        {
            return ConvertibleIterator.iterateAsPbRow(this.source.iterator());
        }
    }

    private static class ImmutableCellIterable extends ConvertibleIterable<RiakTsPB.TsCell, Cell>
    {
        public ImmutableCellIterable(Iterable<RiakTsPB.TsCell> source)
        {
            super(source);
        }

        @Override
        public Iterator<Cell> iterator()
        {
            return ConvertibleIterator.iterateAsCell(this.source.iterator());
        }
    }

    public static ConvertibleIterable<Row, RiakTsPB.TsRow> iterateAsPbRow(Iterable<Row> iterable)
    {
        return new ImmutablePBRowIterable(iterable);
    }

    public static ConvertibleIterable<Cell, RiakTsPB.TsCell> iterateAsPbCell(Iterable<Cell> iterable)
    {
        return new ImmutablePBCellIterable(iterable);
    }

    public static ConvertibleIterable<RiakTsPB.TsCell, Cell> iterateAsCell(Iterable<RiakTsPB.TsCell> iterable)
    {
        return new ImmutableCellIterable(iterable);
    }

}
