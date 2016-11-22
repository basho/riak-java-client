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
            return ConvertibleIteratorUtils.iterateAsPbCell(this.source.iterator());
        }
    }

    public static ConvertibleIterable<Cell, RiakTsPB.TsCell> asIterablePbCell(Iterable<Cell> iterable)
    {
        return new ImmutableIterablePBCell(iterable);
    }
}
