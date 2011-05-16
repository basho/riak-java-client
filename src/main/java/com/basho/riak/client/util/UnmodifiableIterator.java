/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.util;

import java.util.Iterator;

/**
 * Decorates an iterator so that remove throws {@link UnsupportedOperationException}
 * @author russell
 * @param <E>
 */
public class UnmodifiableIterator<E> implements Iterator<E> {
    
    private final Iterator<E> delegate;
    

    /**
     * Create a new {@link UnmodifiableIterator} for the given <code>delegate</code>
     * @param delegate the iterator to decorate
     */
    public UnmodifiableIterator(Iterator<E> delegate) {
        this.delegate = delegate;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        return delegate.hasNext();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public E next() {
        return delegate.next();
    }

    /**
     * Always throws (as this is an UnmodifiableIterator).
     * @throws UnsupportedOperationException
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
