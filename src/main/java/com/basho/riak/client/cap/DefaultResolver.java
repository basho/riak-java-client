package com.basho.riak.client.cap;

import java.util.Collection;

/**
 * A conflict resolver that doesn't resolve conflict. If it is presented with a
 * collection of siblings with more than one entry it throws an {@link UnresolvedConflictException}.
 * 
 * @author russell
 * 
 * @see UnresolvedConflictException
 */
public final class DefaultResolver<T> implements ConflictResolver<T> {

    /**
     * Detects conflict but does not resolve it.
     * @return null or the single value in the collection
     * @throws UnresolvedConflictException if <code>siblings</code> as > 1 entry.
     */
    public T resolve(Collection<T> siblings) {
        if (siblings.size() > 1) {
            throw new UnresolvedConflictException("Siblings found", siblings);
        } else if (siblings.size() == 1) {
            return siblings.iterator().next();
        } else {
            return null;
        }
    }
}