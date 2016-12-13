package com.basho.riak.client.core.query;

import java.util.Iterator;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public abstract class ConvertibleIterator<S,D> implements Iterator<D>
{
    protected final Iterator<S> iterator;

    public ConvertibleIterator(Iterator<S> iterator) {
        this.iterator = iterator;
    }

    abstract protected D convert(S source);

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public D next() {
        return convert(iterator.next());
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException();
    }
}
