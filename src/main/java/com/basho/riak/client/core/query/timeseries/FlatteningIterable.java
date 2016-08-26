package com.basho.riak.client.core.query.timeseries;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by alex on 12/8/15.
 */
public class FlatteningIterable<O,I> implements Iterable<I>
{
    public interface InnerIterableProvider<O,I>
    {
        Iterator<I> getInnerIterator(O provider);
    }

    private final Iterable<O> source;
    private final InnerIterableProvider<O, I> innerIterableProvider;

    public FlatteningIterable(Iterable<O> source, InnerIterableProvider<O,I> innerIterableProvider)
    {
        this.source = source;
        this.innerIterableProvider = innerIterableProvider;
    }

    @Override
    public Iterator<I> iterator()
    {
        return new FlatteningIterator<>(source.iterator(), innerIterableProvider);
    }

    private static class FlatteningIterator<O,I> implements Iterator<I>
    {
        private final Iterator<O> iteratorSource;
        private final InnerIterableProvider<O, I> innerIterableProvider;
        private Iterator<I> currentIterator = null;

        public FlatteningIterator(Iterator<O> source, InnerIterableProvider<O,I> innerIterableProvider)
        {
            this.iteratorSource = source;
            this.innerIterableProvider = innerIterableProvider;
            loadNextIterator();
        }

        @Override
        public boolean hasNext()
        {
            if(currentIteratorHasMore())
            {
                return true;
            }
            else if(currentIteratorIsEmptyButSourceHasMore())
            {
                loadNextIterator();
                return hasNext();
            }
            else
            {
                return false;
            }
        }

        @Override
        public I next()
        {
            if(currentIteratorHasMore())
            {
                return currentIterator.next();
            }
            else if(currentIteratorIsEmptyButSourceHasMore())
            {
                loadNextIterator();
                return next();
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private boolean currentIteratorHasMore()
        {
            return this.currentIterator != null &&
                   this.currentIterator.hasNext();
        }

        private boolean currentIteratorIsEmptyButSourceHasMore()
        {
            return currentIterator != null &&
                    !currentIterator.hasNext() &&
                    iteratorSource.hasNext();
        }

        private void loadNextIterator()
        {
            if(this.iteratorSource.hasNext())
            {
                this.currentIterator = innerIterableProvider.getInnerIterator(this.iteratorSource.next());
            }
            else
            {
                this.currentIterator = null;
            }
        }
    }
}
